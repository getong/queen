// wl — a small paced workload for a Queen 2 cluster: -queues queues, -partitions partitions in total, pushes at a
// fixed rate, consumers that pop and ack. Every -report it prints messages per second and latency.
//
// Producer (open loop): the pacer schedules one push every batch/rate seconds and never waits for an answer: each
// push is its own goroutine under the -max-inflight cap, and a push scheduled while the cap is full is counted as
// shed, never sent. Push latency runs from the SCHEDULED instant to the broker's answer, so a slow broker cannot
// hide behind a client that slowed down with it.
//
// Consumers (closed loop): -consumers per queue, queue mode, long-poll pop over -pop-width partitions, then one
// /ack/batch sent off the pop loop. End-to-end latency = pop answer received minus the message's scheduled instant (one process, one
// clock).
//
// With -brokers the run also checks the broker's memory rule (2.2.0): every push leaves one row in the RAM of
// every node for the queue's window = max(dedupWindowSeconds, the broker's floor). Until 2.2.0-beta.1 a longer
// completedRetentionSeconds was a third term: -completed-holds-rows models that. Each
// report reads the row count of each broker and sets it against the model: this run's own acked pushes of the
// last window seconds, per queue. -linger watches the rows leave after the last push, and -late-queues reads a
// queue only then, when none of its messages has a row left.
//
// Standard library only: the pod builds it from a ConfigMap with no network (see run.sh).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// histogram: log buckets 2% wide, lock-free

const hBuckets = 1200

var hK = 1 / math.Log(1.02)

type hist struct {
	b   [hBuckets]atomic.Uint64
	n   atomic.Uint64
	max atomic.Uint64
}

func (h *hist) add(us int64) {
	if us < 1 {
		us = 1
	}
	i := int(math.Log(float64(us)) * hK)
	if i >= hBuckets {
		i = hBuckets - 1
	}
	h.b[i].Add(1)
	h.n.Add(1)
	for {
		m := h.max.Load()
		if uint64(us) <= m || h.max.CompareAndSwap(m, uint64(us)) {
			return
		}
	}
}

type snap struct {
	b   []uint64
	n   uint64
	max uint64
}

// take copies the histogram; reset empties it for the next window.
func (h *hist) take(reset bool) snap {
	s := snap{b: make([]uint64, hBuckets)}
	for i := range h.b {
		if reset {
			s.b[i] = h.b[i].Swap(0)
		} else {
			s.b[i] = h.b[i].Load()
		}
		s.n += s.b[i]
	}
	if reset {
		s.max = h.max.Swap(0)
		h.n.Store(0)
	} else {
		s.max = h.max.Load()
	}
	return s
}

// ms returns percentile p (0..100) in milliseconds.
func (s snap) ms(p float64) float64 {
	if s.n == 0 {
		return 0
	}
	want := uint64(math.Ceil(float64(s.n) * p / 100))
	var c uint64
	for i, v := range s.b {
		c += v
		if c >= want && v > 0 {
			us := math.Exp((float64(i) + 0.5) / hK)
			if m := float64(s.max); us > m {
				us = m
			}
			return us / 1000
		}
	}
	return float64(s.max) / 1000
}

func (s snap) maxMs() float64 { return float64(s.max) / 1000 }

// lat is one measure kept twice: the current window and the whole run.
type lat struct{ win, tot hist }

func (l *lat) add(us int64) { l.win.add(us); l.tot.add(us) }

// ---------------------------------------------------------------------------

type wl struct {
	urls      []string
	prefix    string
	queues    int
	perQueue  int
	rate      float64
	batch     int
	pad       string
	consumers int
	popBatch  int
	popWidth  int
	popWaitMs int
	lease     int
	timeout   time.Duration

	hc     *http.Client
	rr     atomic.Uint64
	sem    chan struct{}
	ackSem chan struct{}
	ackWg  sync.WaitGroup

	// the rows model (-brokers)
	brokers  []string
	windowS  []int // per queue: seconds a push's row stays in RAM
	late     int   // the last `late` queues get their consumers only after -linger
	t0       time.Time
	perSec   [][]atomic.Uint32 // [queue][second since START]: pushes acked
	base     []bstat           // each broker before START
	stopRSS  float64
	stopProd atomic.Bool
	samples  []sample
	pushEnd  int // index in samples of the last report while pushing

	pushed, pushErr, shed atomic.Uint64 // messages
	pushedLate, latePop   atomic.Uint64 // messages of the late queues
	popped, warmSeen      atomic.Uint64
	ackErr, popErr        atomic.Uint64 // calls
	perQueuePush          []atomic.Uint64
	perQueuePop           []atomic.Uint64
	pushLat, e2e, ackLat  lat

	errMu   sync.Mutex
	errs    map[string]int
	errShow int
}

func (w *wl) queue(i int) string { return fmt.Sprintf("%s-%d", w.prefix, i) }

func (w *wl) fail(kind string, err error) {
	key := kind
	var he *httpErr
	var ne net.Error
	if errors.As(err, &he) {
		key = fmt.Sprintf("%s HTTP %d", kind, he.code)
	} else if errors.As(err, &ne) && ne.Timeout() {
		key = kind + " timeout"
	}
	w.errMu.Lock()
	w.errs[key]++
	show := w.errShow < 10
	if show {
		w.errShow++
	}
	w.errMu.Unlock()
	if show {
		fmt.Fprintf(os.Stderr, "  [error] %s: %v\n", kind, err)
	}
}

type httpErr struct {
	code int
	body string
}

func (e *httpErr) Error() string { return fmt.Sprintf("HTTP %d: %s", e.code, e.body) }

// do issues one request to the next broker URL; a status outside 2xx is an *httpErr carrying the body.
func (w *wl) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	base := w.urls[int(w.rr.Add(1))%len(w.urls)]
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := w.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	rb, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, rb, nil
	}
	b := string(bytes.TrimSpace(rb))
	if len(b) > 200 {
		b = b[:200] + "..."
	}
	return resp.StatusCode, rb, &httpErr{resp.StatusCode, b}
}

// ---------------------------------------------------------------------------
// setup

func (w *wl) waitHealthy(ctx context.Context) error {
	dl := time.Now().Add(60 * time.Second)
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, rb, err := w.do(cctx, "GET", "/health", nil)
		cancel()
		if err == nil {
			var h struct {
				Status  string `json:"status"`
				Version string `json:"version"`
			}
			if json.Unmarshal(rb, &h) == nil && h.Status == "healthy" {
				fmt.Printf("  [queen] healthy, version %s (%s)\n", h.Version, strings.Join(w.urls, ", "))
				return nil
			}
		}
		if time.Now().After(dl) || ctx.Err() != nil {
			return fmt.Errorf("broker not healthy within 60s: %v %s", err, rb)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// configure sets every queue's lease, retention and dedup window. Consumed messages go after their queue's
// completed retention, anything never consumed after pendingRet.
func (w *wl) configure(ctx context.Context, dedup, completedRet []int, pendingRet int) error {
	for i := 0; i < w.queues; i++ {
		body := fmt.Sprintf(`{"queue":%q,"options":{"leaseTime":%d,"retentionEnabled":true,"completedRetentionSeconds":%d,"retentionSeconds":%d,"dedupWindowSeconds":%d}}`,
			w.queue(i), w.lease, completedRet[i], pendingRet, dedup[i])
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, _, err := w.do(cctx, "POST", "/api/v1/configure", []byte(body))
		cancel()
		if err != nil {
			return fmt.Errorf("configure %s: %v", w.queue(i), err)
		}
		fmt.Printf("  [configure] %s: leaseTime %ds, dedupWindowSeconds %d, completedRetentionSeconds %d, retentionSeconds %d -> a push's row stays %ds\n",
			w.queue(i), w.lease, dedup[i], completedRet[i], pendingRet, w.windowS[i])
	}
	return nil
}

// warm pushes one message without "ts" into every partition (500 per request), so the measured run meets
// partitions that exist. The consumers take them and do not count them.
func (w *wl) warm(ctx context.Context) error {
	t := time.Now()
	total := uint64((w.queues - w.late) * w.perQueue)
	for q := 0; q < w.queues-w.late; q++ {
		for lo := 0; lo < w.perQueue; lo += 500 {
			hi := min(lo+500, w.perQueue)
			b := []byte(`{"items":[`)
			for p := lo; p < hi; p++ {
				if p > lo {
					b = append(b, ',')
				}
				b = fmt.Appendf(b, `{"queue":%q,"partition":"p%d","payload":{"warm":1}}`, w.queue(q), p)
			}
			b = append(b, `]}`...)
			cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
			_, _, err := w.do(cctx, "POST", "/api/v1/push", b)
			cancel()
			if err != nil {
				return fmt.Errorf("warm push %s: %v", w.queue(q), err)
			}
		}
	}
	pushS := time.Since(t).Seconds()
	for w.warmSeen.Load() < total && time.Since(t) < 120*time.Second && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("  [warm] %d partitions created in %.1fs; %d of %d warm-up messages consumed after %.1fs\n",
		total, pushS, w.warmSeen.Load(), total, time.Since(t).Seconds())
	return ctx.Err()
}

func (w *wl) clean(ctx context.Context) {
	for i := 0; i < w.queues; i++ {
		cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		code, rb, err := w.do(cctx, "DELETE", "/api/v1/resources/queues/"+url.PathEscape(w.queue(i)), nil)
		cancel()
		if err != nil {
			fmt.Printf("  [clean] %s: %v\n", w.queue(i), err)
			continue
		}
		fmt.Printf("  [clean] %s deleted (HTTP %d) %s\n", w.queue(i), code, bytes.TrimSpace(rb))
	}
}

// ---------------------------------------------------------------------------
// produce

// produce paces units from start for dur (0 = until ctx ends). A late pacer catches up at once: the schedule is
// wall-clock, so the offered rate holds.
func (w *wl) produce(ctx context.Context, start time.Time, dur time.Duration) {
	stepNs := 1e9 * float64(w.batch) / w.rate
	for i := int64(0); ; i++ {
		off := time.Duration(float64(i) * stepNs)
		if dur > 0 && off >= dur {
			return
		}
		sched := start.Add(off)
		if d := time.Until(sched); d > 0 {
			time.Sleep(d)
		}
		if ctx.Err() != nil || w.stopProd.Load() {
			return
		}
		q := int(i % int64(w.queues))
		p := int((i / int64(w.queues)) % int64(w.perQueue))
		select {
		case w.sem <- struct{}{}:
			go w.push(q, p, sched)
		default:
			w.shed.Add(uint64(w.batch))
		}
	}
}

func (w *wl) push(q, p int, sched time.Time) {
	defer func() { <-w.sem }()
	name := w.queue(q)
	ts := sched.UnixMicro()
	b := make([]byte, 0, 32+w.batch*(len(name)+len(w.pad)+96))
	b = append(b, `{"items":[`...)
	for j := 0; j < w.batch; j++ {
		if j > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"queue":"`...)
		b = append(b, name...)
		b = append(b, `","partition":"p`...)
		b = strconv.AppendInt(b, int64(p), 10)
		b = append(b, `","payload":{"ts":`...)
		b = strconv.AppendInt(b, ts, 10)
		b = append(b, `,"pad":"`...)
		b = append(b, w.pad...)
		b = append(b, `"}}`...)
	}
	b = append(b, `]}`...)
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	_, rb, err := w.do(ctx, "POST", "/api/v1/push", b)
	cancel()
	if err == nil {
		var items []struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if jerr := json.Unmarshal(rb, &items); jerr != nil {
			err = fmt.Errorf("push answer: %v", jerr)
		} else if len(items) != w.batch {
			err = fmt.Errorf("push answered %d items for %d", len(items), w.batch)
		} else {
			for _, it := range items {
				if it.Status != "queued" && it.Status != "duplicate" {
					err = fmt.Errorf("push item status %q: %s", it.Status, it.Error)
					break
				}
			}
		}
	}
	if err != nil {
		w.pushErr.Add(uint64(w.batch))
		w.fail("push", err)
		return
	}
	w.pushLat.add(time.Since(sched).Microseconds())
	w.pushed.Add(uint64(w.batch))
	w.perQueuePush[q].Add(uint64(w.batch))
	if q >= w.queues-w.late {
		w.pushedLate.Add(uint64(w.batch))
	}
	if w.perSec != nil {
		if sec := int(time.Since(w.t0).Seconds()); sec >= 0 && sec < len(w.perSec[q]) {
			w.perSec[q][sec].Add(1) // one push = one row, whatever -batch
		}
	}
}

// ---------------------------------------------------------------------------
// consume

type qmsg struct {
	TransactionID string          `json:"transactionId"`
	PartitionID   string          `json:"partitionId"`
	LeaseID       string          `json:"leaseId"`
	Data          json.RawMessage `json:"data"`
}

func (w *wl) consume(ctx context.Context, q int, wg *sync.WaitGroup) {
	defer wg.Done()
	late := q >= w.queues-w.late
	path := fmt.Sprintf("/api/v1/pop/queue/%s?batch=%d&partitions=%d&wait=true&timeout=%d&autoAck=false&leaseSeconds=%d",
		url.PathEscape(w.queue(q)), w.popBatch, w.popWidth, w.popWaitMs, w.lease)
	for ctx.Err() == nil {
		_, rb, err := w.do(ctx, "GET", path, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.popErr.Add(1)
			w.fail("pop", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(rb) == 0 {
			continue
		}
		var r struct {
			Messages []qmsg `json:"messages"`
		}
		if err := json.Unmarshal(rb, &r); err != nil {
			w.popErr.Add(1)
			w.fail("pop", fmt.Errorf("pop answer: %v", err))
			continue
		}
		if len(r.Messages) == 0 {
			continue
		}
		now := time.Now().UnixMicro()
		load, warm := 0, 0
		for i := range r.Messages {
			var d struct {
				Ts int64 `json:"ts"`
			}
			_ = json.Unmarshal(r.Messages[i].Data, &d)
			if d.Ts == 0 {
				warm++
				continue
			}
			load++
			if !late {
				w.e2e.add(now - d.Ts)
			}
		}
		b := make([]byte, 0, 32+len(r.Messages)*190)
		b = append(b, `{"acknowledgments":[`...)
		for i := range r.Messages {
			m := &r.Messages[i]
			if i > 0 {
				b = append(b, ',')
			}
			b = fmt.Appendf(b, `{"transactionId":%q,"partitionId":%q,"leaseId":%q,"status":"completed"}`, m.TransactionID, m.PartitionID, m.LeaseID)
		}
		b = append(b, `]}`...)
		// Acked off the pop loop, so the consumer is back on its long poll at once and end-to-end latency is the
		// delivery, not the ack round trip. -ack-inflight full = the consumer waits. The ack outlives a stop
		// signal: a message popped is a message acked.
		w.ackSem <- struct{}{}
		w.ackWg.Add(1)
		go func() {
			defer w.ackWg.Done()
			defer func() { <-w.ackSem }()
			actx, cancel := context.WithTimeout(context.Background(), w.timeout)
			t := time.Now()
			_, ab, err := w.do(actx, "POST", "/api/v1/ack/batch", b)
			cancel()
			if err == nil {
				var items []struct {
					Success bool   `json:"success"`
					Error   string `json:"error"`
				}
				if jerr := json.Unmarshal(ab, &items); jerr != nil {
					err = fmt.Errorf("ack answer: %v", jerr)
				} else {
					for _, it := range items {
						if !it.Success {
							err = fmt.Errorf("ack refused: %s", it.Error)
							break
						}
					}
				}
			}
			if err != nil {
				w.ackErr.Add(1)
				w.fail("ack", err)
				return
			}
			w.ackLat.add(time.Since(t).Microseconds())
			if late {
				w.latePop.Add(uint64(load))
			} else {
				w.popped.Add(uint64(load))
			}
			w.perQueuePop[q].Add(uint64(load))
			w.warmSeen.Add(uint64(warm))
		}()
	}
}

// ---------------------------------------------------------------------------
// brokers: the rows in RAM against the model

type bstat struct {
	rows, cold uint64
	rssMB      float64
	ok         bool
}

type sample struct {
	at    float64 // seconds since START
	rows  []int64 // per broker, minus what was there before START
	model uint64
}

func metricValue(l string) float64 {
	v, _ := strconv.ParseFloat(l[strings.LastIndexByte(l, ' ')+1:], 64)
	return v
}

// scrape reads one broker's /metrics/prometheus: the rows of the txns keyspace (one per push), the resident
// memory, and the claims served from the queue log.
func (w *wl) scrape(base string) bstat {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", base+"/metrics/prometheus", nil)
	if err != nil {
		return bstat{}
	}
	resp, err := w.hc.Do(req)
	if err != nil {
		return bstat{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return bstat{}
	}
	b := bstat{ok: true}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		l := sc.Text()
		switch {
		case strings.HasPrefix(l, `queen_raft_store_ram_rows{ks="txns",kind="live"} `):
			b.rows = uint64(metricValue(l))
		case strings.HasPrefix(l, "queen_consume_cold_claims_total "):
			b.cold = uint64(metricValue(l))
		case strings.HasPrefix(l, "queen_process_resident_memory_bytes "):
			b.rssMB = metricValue(l) / (1 << 20)
		}
	}
	return b
}

func (w *wl) scrapeAll() []bstat {
	out := make([]bstat, len(w.brokers))
	var wg sync.WaitGroup
	for i := range w.brokers {
		wg.Add(1)
		go func() { defer wg.Done(); out[i] = w.scrape(w.brokers[i]) }()
	}
	wg.Wait()
	return out
}

// model is the number of rows this run's pushes should have in RAM now: every acked push younger than its queue's
// window, counted in whole seconds (a report falls on a second boundary, so the count is the window's). The broker
// deletes a row in the retention round after its time (every 5 s by default), so once rows leave, the measured
// count runs up to a few seconds of pushes above the model.
func (w *wl) model() uint64 {
	now := int(time.Since(w.t0).Seconds())
	var n uint64
	for q := range w.perSec {
		for sec := max(now-w.windowS[q], 0); sec <= now && sec < len(w.perSec[q]); sec++ {
			n += uint64(w.perSec[q][sec].Load())
		}
	}
	return n
}

// observe takes one sample of every broker and returns it with its text for the report line.
func (w *wl) observe() (sample, []bstat, string) {
	st := w.scrapeAll()
	sm := sample{at: time.Since(w.t0).Seconds(), model: w.model(), rows: make([]int64, len(st))}
	rows, rss := make([]string, len(st)), make([]string, len(st))
	var cold uint64
	for i, b := range st {
		if !b.ok {
			rows[i], rss[i], sm.rows[i] = "?", "?", -1
			continue
		}
		sm.rows[i] = int64(b.rows) - int64(w.base[i].rows)
		rows[i], rss[i] = strconv.FormatInt(sm.rows[i], 10), strconv.FormatFloat(b.rssMB, 'f', 0, 64)
		cold += b.cold - min(w.base[i].cold, b.cold)
	}
	return sm, st, fmt.Sprintf(" | rows %s model %d | rss MB %s | cold claims %d", strings.Join(rows, "/"), sm.model, strings.Join(rss, "/"), cold)
}

func fmtRows(r []int64) string {
	out := make([]string, len(r))
	for i, v := range r {
		out[i] = strconv.FormatInt(v, 10)
	}
	return strings.Join(out, " / ")
}

// ---------------------------------------------------------------------------
// report

func (w *wl) reporter(start time.Time, every time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	var lastPush, lastPop, lastErr, lastShed uint64
	last := start
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-tick.C:
			s := now.Sub(last).Seconds()
			last = now
			pu, po, pe, sh := w.pushed.Load(), w.popped.Load(), w.pushErr.Load(), w.shed.Load()
			lateIn, lateOut := w.pushedLate.Load(), w.latePop.Load()
			pl, el := w.pushLat.win.take(true), w.e2e.win.take(true)
			var tail string
			if w.late > 0 {
				tail = fmt.Sprintf(" | late queues wait %d", int64(lateIn)-int64(lateOut))
			}
			if len(w.brokers) > 0 {
				sm, st, text := w.observe()
				tail += text
				w.samples = append(w.samples, sm)
				if pu > lastPush {
					w.pushEnd = len(w.samples) - 1
				}
				for i, b := range st {
					if w.stopRSS > 0 && b.rssMB > w.stopRSS && !w.stopProd.Swap(true) {
						fmt.Printf("STOP PUSHING: broker %d holds %.0f MB resident, above -stop-rss-mb %.0f\n", i, b.rssMB, w.stopRSS)
					}
				}
			}
			fmt.Printf("[%s] push %7.0f/s  pop %7.0f/s  backlog %6d | push ms p50 %6.2f p99 %7.2f max %7.1f | e2e ms p50 %7.2f p99 %8.2f max %8.1f | failed %d shed %d popErr %d ackErr %d%s\n",
				now.UTC().Format("15:04:05"), float64(pu-lastPush)/s, float64(po-lastPop)/s, int64(pu)-int64(lateIn)-int64(po),
				pl.ms(50), pl.ms(99), pl.maxMs(), el.ms(50), el.ms(99), el.maxMs(),
				pe-lastErr, sh-lastShed, w.popErr.Load(), w.ackErr.Load(), tail)
			lastPush, lastPop, lastErr, lastShed = pu, po, pe, sh
		}
	}
}

func row(name string, s snap) {
	fmt.Printf("  %-18s p50 %8.2f   p90 %8.2f   p99 %8.2f   p99.9 %8.2f   max %8.1f   (n=%d)\n",
		name, s.ms(50), s.ms(90), s.ms(99), s.ms(99.9), s.maxMs(), s.n)
}

type lateResult struct {
	ran     bool
	seconds float64
	cold    uint64 // claims served from the queue log while the late queues were read, all brokers
}

func (w *wl) summary(produceS, totalS float64, lr lateResult) {
	pu, po := w.pushed.Load()-w.pushedLate.Load(), w.popped.Load()
	fmt.Printf("\n== result ==\n")
	fmt.Printf("  %d queues (%s..%s) x %d partitions = %d | offered %.0f msg/s, %d per push, payload %d B | %d consumers per queue | %.0fs\n",
		w.queues, w.queue(0), w.queue(w.queues-1), w.perQueue, w.queues*w.perQueue, w.rate, w.batch, len(w.pad)+32, w.consumers, produceS)
	fmt.Printf("  pushed    %10d msgs  %9.1f/s   failed %d   shed %d\n", w.pushed.Load(), float64(w.pushed.Load())/produceS, w.pushErr.Load(), w.shed.Load())
	fmt.Printf("  consumed  %10d msgs  %9.1f/s   left %d   pop errors %d   ack errors %d\n", po, float64(po)/totalS, int64(pu)-int64(po), w.popErr.Load(), w.ackErr.Load())
	fmt.Printf("  latency, ms\n")
	row("push", w.pushLat.tot.take(false))
	row("end to end", w.e2e.tot.take(false))
	row("ack call", w.ackLat.tot.take(false))
	fmt.Printf("  per queue (pushed/consumed):")
	for i := 0; i < w.queues; i++ {
		fmt.Printf("  %s %d/%d", w.queue(i), w.perQueuePush[i].Load(), w.perQueuePop[i].Load())
	}
	fmt.Println()
	if len(w.samples) > 0 {
		peak := 0
		for i, sm := range w.samples {
			if sm.model > w.samples[peak].model {
				peak = i
			}
		}
		fmt.Printf("  rows in RAM, one per push (queen_raft_store_ram_rows ks=txns live, minus the rows of before START), per broker, against the model\n")
		fmt.Printf("    row window per queue:")
		for i := 0; i < w.queues; i++ {
			fmt.Printf("  %s %ds", w.queue(i), w.windowS[i])
		}
		fmt.Println()
		line := func(name string, sm sample) {
			fmt.Printf("    %-22s at %5.0fs   measured %s   model %d\n", name, sm.at, fmtRows(sm.rows), sm.model)
		}
		line("highest model", w.samples[peak])
		line("last report of pushing", w.samples[w.pushEnd])
		line("end", w.samples[len(w.samples)-1])
	}
	if w.late > 0 {
		pl, cl := w.pushedLate.Load(), w.latePop.Load()
		if lr.ran {
			fmt.Printf("  late queues (%s..%s), read only after the watch: pushed %d, consumed %d, left %d, in %.1fs (%.0f msg/s); claims served from the queue log meanwhile: %d\n",
				w.queue(w.queues-w.late), w.queue(w.queues-1), pl, cl, int64(pl)-int64(cl), lr.seconds, float64(cl)/max(lr.seconds, 0.001), lr.cold)
		} else {
			fmt.Printf("  late queues (%s..%s): pushed %d, NOT read (the run was stopped before)\n", w.queue(w.queues-w.late), w.queue(w.queues-1), pl)
		}
	}
	w.errMu.Lock()
	if len(w.errs) == 0 {
		fmt.Printf("  errors: none\n")
	} else {
		keys := make([]string, 0, len(w.errs))
		for k := range w.errs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Printf("  errors:")
		for _, k := range keys {
			fmt.Printf("  %s x%d", k, w.errs[k])
		}
		fmt.Println()
	}
	w.errMu.Unlock()
}

// ---------------------------------------------------------------------------

// listPerQueue reads "v" (every queue) or "v0,v1,..." (one per queue).
func listPerQueue(s string, n int, name string) ([]int, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 1 && len(parts) != n {
		return nil, fmt.Errorf("-%s wants one value or %d, one per queue", name, n)
	}
	out := make([]int, n)
	for i := range out {
		p := strings.TrimSpace(parts[min(i, len(parts)-1)])
		v, err := strconv.Atoi(p)
		if err != nil || v < 0 {
			return nil, fmt.Errorf("-%s: %q is not a number of seconds", name, p)
		}
		out[i] = v
	}
	return out, nil
}

func main() {
	urls := flag.String("url", "http://127.0.0.1:6632", "broker base URLs, comma-separated (requests go round robin)")
	prefix := flag.String("prefix", "wl", "queue name prefix: <prefix>-0 .. <prefix>-<queues-1>")
	queues := flag.Int("queues", 5, "queues")
	partitions := flag.Int("partitions", 10000, "partitions in TOTAL, split evenly over the queues")
	rate := flag.Float64("rate", 1000, "offered messages per second, all queues together")
	batch := flag.Int("batch", 1, "messages per push (one partition)")
	payload := flag.Int("payload", 256, "payload bytes, about")
	consumers := flag.Int("consumers", 4, "consumers per queue (0 = produce only)")
	popBatch := flag.Int("pop-batch", 100, "messages per pop")
	popWidth := flag.Int("pop-width", 10, "partitions claimed per pop")
	popWait := flag.Duration("pop-wait", 5*time.Second, "long-poll wait of an empty pop")
	lease := flag.Int("lease", 30, "lease seconds of a pop, and the queues' leaseTime")
	duration := flag.Duration("duration", 10*time.Minute, "producing time (0 = until SIGTERM)")
	report := flag.Duration("report", 10*time.Second, "one line every this long")
	drain := flag.Duration("drain", 15*time.Second, "after producing: consumers get this long to empty the queues")
	ackInflight := flag.Int("ack-inflight", 128, "ack calls in flight; full = the consumers wait")
	maxInflight := flag.Int("max-inflight", 512, "pushes in flight; a push scheduled above it is shed, not sent")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	completedRetF := flag.String("completed-retention", "300", "configure: consumed messages are kept this many seconds; one value, or one per queue (comma-separated)")
	dedupF := flag.String("dedup-window", "0", "configure: dedupWindowSeconds; one value, or one per queue (the brokers' default for a queue nobody configured is 3600)")
	floor := flag.Int("floor", 900, "the brokers' QUEEN_RAFT_TXN_WINDOW_MIN_S, for the rows model")
	completedHolds := flag.Bool("completed-holds-rows", false, "rows model of 2.2.0-beta.1 and before: a completed retention longer than the dedup window and the floor also keeps the row")
	brokersF := flag.String("brokers", "", "every broker's own base URL, comma-separated: each report reads its rows in RAM, resident memory and cold claims, and sets the rows against the model")
	linger := flag.Duration("linger", 0, "after producing: keep reporting this long, to watch the rows leave")
	late := flag.Int("late-queues", 0, "the last N queues get no consumer until -linger is over, then they are read to the end")
	stopRSS := flag.Float64("stop-rss-mb", 0, "with -brokers: stop producing when a broker's resident memory passes this many MB (0 = never)")
	pendingRet := flag.Int("retention", 3600, "configure: a message nobody consumed is dropped after this many seconds")
	doWarm := flag.Bool("warm", true, "create every partition before the measured run")
	doClean := flag.Bool("clean", false, "delete the queues and exit")
	flag.Parse()

	w := &wl{prefix: *prefix, queues: *queues, rate: *rate, batch: *batch, consumers: *consumers, popBatch: *popBatch,
		popWidth: *popWidth, popWaitMs: int(popWait.Milliseconds()), lease: *lease, timeout: *timeout, errs: map[string]int{},
		late: *late, stopRSS: *stopRSS}
	for _, u := range strings.Split(*brokersF, ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			w.brokers = append(w.brokers, u)
		}
	}
	for _, u := range strings.Split(*urls, ",") {
		if u = strings.TrimRight(strings.TrimSpace(u), "/"); u != "" {
			w.urls = append(w.urls, u)
		}
	}
	if len(w.urls) == 0 || w.queues < 1 || *partitions < w.queues || w.rate <= 0 || w.batch < 1 || w.late < 0 || w.late >= w.queues {
		fmt.Println("FATAL need -url, -queues >= 1, -partitions >= -queues, -rate > 0, -batch >= 1, -late-queues < -queues")
		os.Exit(2)
	}
	dedup, err1 := listPerQueue(*dedupF, w.queues, "dedup-window")
	completedRet, err2 := listPerQueue(*completedRetF, w.queues, "completed-retention")
	if err := errors.Join(err1, err2); err != nil {
		fmt.Println("FATAL", err)
		os.Exit(2)
	}
	w.windowS = make([]int, w.queues)
	for i := range w.windowS {
		w.windowS[i] = max(dedup[i], *floor)
		if *completedHolds {
			w.windowS[i] = max(w.windowS[i], completedRet[i])
		}
	}
	w.perQueue = *partitions / w.queues
	w.pad = strings.Repeat("x", max(*payload-32, 0))
	w.sem = make(chan struct{}, *maxInflight)
	w.ackSem = make(chan struct{}, *ackInflight)
	w.perQueuePush = make([]atomic.Uint64, w.queues)
	w.perQueuePop = make([]atomic.Uint64, w.queues)
	conns := *maxInflight + *ackInflight + w.queues*w.consumers + 16
	w.hc = &http.Client{Timeout: *timeout + *popWait, Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        conns,
		MaxIdleConnsPerHost: conns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("wl: %d queues x %d partitions = %d | %.0f msg/s, %d per push, ~%d B | %d consumers per queue (pop batch %d, width %d) | %v\n",
		w.queues, w.perQueue, w.queues*w.perQueue, w.rate, w.batch, len(w.pad)+32, w.consumers, w.popBatch, w.popWidth, *duration)
	if err := w.waitHealthy(ctx); err != nil {
		fmt.Println("FATAL", err)
		os.Exit(1)
	}
	if *doClean {
		w.clean(ctx)
		return
	}
	if err := w.configure(ctx, dedup, completedRet, *pendingRet); err != nil {
		fmt.Println("FATAL", err)
		os.Exit(1)
	}
	if len(w.brokers) > 0 {
		w.base = w.scrapeAll()
		for i, b := range w.base {
			if !b.ok {
				fmt.Printf("FATAL cannot read %s/metrics/prometheus\n", w.brokers[i])
				os.Exit(1)
			}
			fmt.Printf("  [broker %d] before the run: %d rows, %.0f MB resident, %d cold claims (%s)\n", i, b.rows, b.rssMB, b.cold, w.brokers[i])
		}
		secs := 86400
		if *duration > 0 {
			secs = int((*duration + *linger + *drain).Seconds()) + 1800
		}
		w.perSec = make([][]atomic.Uint32, w.queues)
		for q := range w.perSec {
			w.perSec[q] = make([]atomic.Uint32, secs)
		}
	}

	cctx, cstop := context.WithCancel(context.Background())
	var cwg sync.WaitGroup
	for q := 0; q < w.queues-w.late; q++ {
		for c := 0; c < w.consumers; c++ {
			cwg.Add(1)
			go w.consume(cctx, q, &cwg)
		}
	}
	if *doWarm && w.consumers > 0 {
		if err := w.warm(ctx); err != nil {
			fmt.Println("FATAL", err)
			os.Exit(1)
		}
	}

	start := time.Now()
	w.t0 = start
	fmt.Printf("START %s\n", start.UTC().Format(time.RFC3339))
	repStop, repDone := make(chan struct{}), make(chan struct{})
	go w.reporter(start, *report, repStop, repDone)
	w.produce(ctx, start, *duration)
	produceS := time.Since(start).Seconds()

	// pushes in flight finish (or time out); then the consumers empty what is left
	for i := 0; i < cap(w.sem); i++ {
		w.sem <- struct{}{}
	}
	dl := time.Now().Add(*drain)
	for w.consumers > 0 && w.popped.Load() < w.pushed.Load()-w.pushedLate.Load() && time.Now().Before(dl) {
		time.Sleep(50 * time.Millisecond)
	}
	totalS := time.Since(start).Seconds()

	if *linger > 0 && ctx.Err() == nil {
		fmt.Printf("PUSHING ENDED %s after %.0fs; watching for %v\n", time.Now().UTC().Format("15:04:05"), produceS, *linger)
		select {
		case <-ctx.Done():
		case <-time.After(*linger):
		}
	}
	// the late queues: read only now, when the rows of their oldest messages have left the brokers' RAM
	var lr lateResult
	if w.late > 0 && w.consumers > 0 && ctx.Err() == nil {
		coldSum := func() (n uint64) {
			for _, b := range w.scrapeAll() {
				n += b.cold
			}
			return
		}
		cold0, t := coldSum(), time.Now()
		fmt.Printf("LATE CONSUMERS START %s: %d messages wait in %s..%s\n", t.UTC().Format("15:04:05"),
			w.pushedLate.Load(), w.queue(w.queues-w.late), w.queue(w.queues-1))
		for q := w.queues - w.late; q < w.queues; q++ {
			for c := 0; c < w.consumers; c++ {
				cwg.Add(1)
				go w.consume(cctx, q, &cwg)
			}
		}
		// until everything came out, or nothing has for a minute
		seen, since := uint64(0), time.Now()
		for w.latePop.Load() < w.pushedLate.Load() && ctx.Err() == nil && time.Since(since) < time.Minute {
			if n := w.latePop.Load(); n != seen {
				seen, since = n, time.Now()
			}
			time.Sleep(50 * time.Millisecond)
		}
		lr = lateResult{ran: true, seconds: time.Since(t).Seconds(), cold: coldSum() - cold0}
	}
	close(repStop)
	<-repDone
	cstop()
	cwg.Wait()
	w.ackWg.Wait()
	if len(w.brokers) > 0 {
		sm, _, _ := w.observe()
		w.samples = append(w.samples, sm)
	}
	w.summary(produceS, totalS, lr)
}
