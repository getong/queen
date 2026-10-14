// kvlockload — load generator for Queen 2.x KV (POST /api/v1/kv), Locks
// (POST /api/v1/locks) and transactions that carry KV or a lock's guard
// (POST /api/v1/transaction).
//
// It replaces 2026-08-20-kv/kvload (Queen 1.0.4, PostgreSQL engine), whose
// three KV shapes it keeps so the two campaigns can be read side by side:
//
//	incr         a fixed-window rate limiter: one call per decision
//	putIfAbsent  an idempotency marker
//	get          a cache read
//
// Modes:
//
//	kv            a weighted mix of incr / get / putIfAbsent / put / delete
//	lock-cycle    acquire then release, every worker on its own locks
//	lock-renew    renew locks that are held (what a fleet of holders costs)
//	lock-contend  many workers on few locks; checks that two never hold one
//	sem           the same on semaphores of -limit permits
//	guard         a KV write behind a lock's guard (-guard=false: without it)
//	txn           a transaction that pushes, with -kvput and/or -guard
//
// Closed loop by default (-workers requests in flight). With -rate the load is
// open loop: operations are scheduled at a fixed pace and latency is measured
// from the scheduled time, so a broker that falls behind shows it.
//
// The warm-up is not measured. Latency is kept in a log-linear histogram
// (0.2% wide buckets), never sampled. Errors are classified, never just
// counted: a run that answered 429s cannot read as a success.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

const (
	histSub    = 512
	histExps   = 28
	histSize   = 1024 + histExps*histSub
	histShards = 16
)

type hist struct {
	shards [histShards][]atomic.Uint64
	sum    [histShards]atomic.Uint64
	max    atomic.Uint64
}

func newHist() *hist {
	h := &hist{}
	for i := range h.shards {
		h.shards[i] = make([]atomic.Uint64, histSize)
	}
	return h
}

func bucketOf(us uint64) int {
	if us < 1024 {
		return int(us)
	}
	e := bits.Len64(us) - 10
	sub := us >> uint(e)
	i := 1024 + (e-1)*histSub + int(sub-512)
	if i >= histSize {
		i = histSize - 1
	}
	return i
}

// The middle of a bucket, in microseconds.
func valueOf(i int) float64 {
	if i < 1024 {
		return float64(i)
	}
	j := i - 1024
	e := j/histSub + 1
	lo := uint64(j%histSub+512) << uint(e)
	return float64(lo) + float64(uint64(1)<<uint(e))/2
}

func (h *hist) add(shard int, d time.Duration) {
	us := uint64(d.Microseconds())
	if d < 0 {
		us = 0
	}
	s := shard % histShards
	h.shards[s][bucketOf(us)].Add(1)
	h.sum[s].Add(us)
	for {
		m := h.max.Load()
		if us <= m || h.max.CompareAndSwap(m, us) {
			break
		}
	}
}

func (h *hist) snapshot() []uint64 {
	out := make([]uint64, histSize)
	for s := range h.shards {
		for i := range h.shards[s] {
			out[i] += h.shards[s][i].Load()
		}
	}
	return out
}

func (h *hist) total() (sumUs uint64) {
	for s := range h.sum {
		sumUs += h.sum[s].Load()
	}
	return
}

func countOf(c []uint64) (n uint64) {
	for _, v := range c {
		n += v
	}
	return
}

// Percentile of a bucket-count vector, in milliseconds.
func pctOf(c []uint64, p float64) float64 {
	n := countOf(c)
	if n == 0 {
		return 0
	}
	want := uint64(float64(n-1)*p) + 1
	var seen uint64
	for i, v := range c {
		seen += v
		if seen >= want {
			return valueOf(i) / 1000.0
		}
	}
	return valueOf(histSize-1) / 1000.0
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

type series struct {
	name string
	h    *hist
	last []uint64
}

type stats struct {
	measureFrom time.Time
	stopAt      time.Time
	series      map[string]*series
	order       []string
	ctr         map[string]*atomic.Int64
	ctrOrder    []string
	errs        sync.Map
	errTotal    atomic.Int64
	maxLagUs    atomic.Int64
}

func newStats(seriesNames, counters []string) *stats {
	st := &stats{series: map[string]*series{}, ctr: map[string]*atomic.Int64{}}
	for _, n := range seriesNames {
		st.series[n] = &series{name: n, h: newHist(), last: make([]uint64, histSize)}
		st.order = append(st.order, n)
	}
	for _, n := range counters {
		st.ctr[n] = new(atomic.Int64)
		st.ctrOrder = append(st.ctrOrder, n)
	}
	return st
}

func (st *stats) measured(t0 time.Time) bool { return !t0.Before(st.measureFrom) }

func (st *stats) rec(name string, w int, t0 time.Time, d time.Duration) {
	if st.measured(t0) {
		st.series[name].h.add(w, d)
	}
}

func (st *stats) add(name string, t0 time.Time, n int64) {
	if n != 0 && st.measured(t0) {
		st.ctr[name].Add(n)
	}
}

func (st *stats) fail(kind string, t0 time.Time) {
	if !st.measured(t0) {
		return
	}
	st.errTotal.Add(1)
	v, _ := st.errs.LoadOrStore(kind, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// A worker's own keep-alive connection, spoken by hand: net/http costs the
// loader about 60 us of CPU a call, which on a machine shared with the broker
// is CPU taken from what is being measured.
type caller struct {
	host string
	w    int
	st   *stats
	conn net.Conn
	br   *bufio.Reader
	req  []byte
	resp []byte
}

func (c *caller) dial() error {
	conn, err := net.DialTimeout("tcp", c.host, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn, c.br = conn, bufio.NewReaderSize(conn, 16<<10)
	return nil
}

func (c *caller) drop() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// roundTrip writes one request and reads its answer into c.resp.
func (c *caller) roundTrip(path string, body []byte) (int, error) {
	if c.conn == nil {
		if err := c.dial(); err != nil {
			return 0, err
		}
	}
	c.req = append(c.req[:0], "POST "...)
	c.req = append(c.req, path...)
	c.req = append(c.req, " HTTP/1.1\r\nHost: "...)
	c.req = append(c.req, c.host...)
	c.req = append(c.req, "\r\nContent-Type: application/json\r\nContent-Length: "...)
	c.req = strconv.AppendInt(c.req, int64(len(body)), 10)
	c.req = append(c.req, "\r\n\r\n"...)
	c.req = append(c.req, body...)
	_ = c.conn.SetDeadline(time.Now().Add(35 * time.Second))
	if _, err := c.conn.Write(c.req); err != nil {
		return 0, err
	}
	line, err := c.br.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	if len(line) < 12 {
		return 0, fmt.Errorf("short status line")
	}
	status, err := strconv.Atoi(string(line[9:12]))
	if err != nil {
		return 0, err
	}
	length, chunked, closing := -1, false, false
	for {
		line, err = c.br.ReadSlice('\n')
		if err != nil {
			return 0, err
		}
		if len(line) <= 2 {
			break
		}
		colon := bytes.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		name, value := line[:colon], bytes.TrimSpace(line[colon+1:])
		switch {
		case bytes.EqualFold(name, []byte("content-length")):
			length, err = strconv.Atoi(string(value))
			if err != nil {
				return 0, err
			}
		case bytes.EqualFold(name, []byte("transfer-encoding")):
			chunked = bytes.EqualFold(value, []byte("chunked"))
		case bytes.EqualFold(name, []byte("connection")):
			closing = bytes.EqualFold(value, []byte("close"))
		}
	}
	c.resp = c.resp[:0]
	switch {
	case chunked:
		for {
			line, err = c.br.ReadSlice('\n')
			if err != nil {
				return 0, err
			}
			n, err := strconv.ParseInt(string(bytes.TrimSpace(line)), 16, 64)
			if err != nil {
				return 0, err
			}
			at := len(c.resp)
			c.resp = append(c.resp, make([]byte, n+2)...)
			if _, err := io.ReadFull(c.br, c.resp[at:]); err != nil {
				return 0, err
			}
			c.resp = c.resp[:at+int(n)]
			if n == 0 {
				break
			}
		}
	case length >= 0:
		if cap(c.resp) < length {
			c.resp = make([]byte, 0, length*2)
		}
		c.resp = c.resp[:length]
		if _, err := io.ReadFull(c.br, c.resp); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("an answer with no length")
	}
	if closing {
		c.drop()
	}
	return status, nil
}

// post sends body and returns the answer's bytes, valid until the next call.
// ok is false when the call failed; the failure is already counted.
func (c *caller) post(path string, body []byte, t0 time.Time) ([]byte, bool) {
	status, err := c.roundTrip(path, body)
	if err != nil {
		c.drop()
		c.st.fail("transport", t0)
		time.Sleep(20 * time.Millisecond)
		return nil, false
	}
	if status != 200 {
		var e struct {
			Error  string `json:"error"`
			Code   string `json:"code"`
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal(c.resp, &e)
		kind := e.Error
		if kind == "" {
			kind = e.Code
		}
		if e.Reason != "" && e.Reason != kind {
			kind += "/" + e.Reason
		}
		c.st.fail(fmt.Sprintf("http_%d:%s", status, kind), t0)
		if status == 429 || status >= 500 {
			time.Sleep(10 * time.Millisecond)
		}
		return nil, false
	}
	return c.resp, true
}

// ---------------------------------------------------------------------------
// Pacing
// ---------------------------------------------------------------------------

type pacer struct {
	start time.Time
	perNs float64
	seq   atomic.Int64
}

// begin gives the next operation its start time. Closed loop: now. Open loop:
// its scheduled time, slept up to. false: the run is over.
func begin(pc *pacer, st *stats) (time.Time, bool) {
	if pc == nil {
		t := time.Now()
		return t, t.Before(st.stopAt)
	}
	n := pc.seq.Add(1) - 1
	due := pc.start.Add(time.Duration(float64(n) * pc.perNs))
	if !due.Before(st.stopAt) {
		return due, false
	}
	if d := time.Until(due); d > 0 {
		time.Sleep(d)
	} else if lag := (-d).Microseconds(); lag > st.maxLagUs.Load() && st.measured(due) {
		st.maxLagUs.Store(lag)
	}
	return due, true
}

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

var (
	fURL      = flag.String("url", "http://127.0.0.1:6632", "broker base URLs, comma separated; worker i calls url[i%n]")
	fMode     = flag.String("mode", "kv", "kv | lock-cycle | lock-renew | lock-contend | sem | guard | txn")
	fTag      = flag.String("tag", "", "label of the run, echoed in the summary")
	fDur      = flag.Int("duration", 60, "measured seconds")
	fWarm     = flag.Int("warmup", 5, "seconds before measuring")
	fWorkers  = flag.Int("workers", 64, "concurrent workers")
	fRate     = flag.Float64("rate", 0, "open loop: operations started per second in total; 0 = closed loop")
	fReport   = flag.Int("report", 10, "seconds between progress lines")
	fBatch    = flag.Int("batch", 1, "kv: operations per call; lock-cycle and lock-renew: locks per call")
	fTTL      = flag.Int("ttl", 60, "ttlSeconds of what is written (keys and locks)")
	fNS       = flag.Int("namespaces", 100, "kv: namespaces")
	fKeys     = flag.Int("keys", 20000, "kv: keys per namespace")
	fIncr     = flag.Int("incr", 60, "kv weight: incr")
	fGet      = flag.Int("get", 30, "kv weight: get")
	fPia      = flag.Int("pia", 10, "kv weight: putIfAbsent")
	fPut      = flag.Int("put", 0, "kv weight: put")
	fDel      = flag.Int("del", 0, "kv weight: delete")
	fValBytes = flag.Int("value-bytes", 16, "bytes of a put's value and of a pushed payload's padding")
	fHeld     = flag.Int("held", 10000, "lock-renew: locks held in total")
	fLocks    = flag.Int("locks", 1, "lock-contend and sem: distinct names")
	fLimit    = flag.Int("limit", 8, "sem: permits of each semaphore")
	fHoldMs   = flag.Float64("hold-ms", 0, "lock-contend and sem: how long a holder keeps the permit")
	fPoll     = flag.String("poll", "sdk", "lock-contend and sem: how a refused acquire comes back: sdk (100 ms, x1.5 to 1 s, jitter) or a pause in ms (0 = at once)")
	fGuard    = flag.Bool("guard", true, "guard and txn: carry the lock's guard")
	fKvPut    = flag.Bool("kvput", false, "txn: carry a KV put")
	fQueue    = flag.String("queue", "kvl-txn", "txn: queue pushed to")
	fForever  = flag.Bool("forever", false, "kv: writes never expire (forever:true) instead of -ttl")
	fOut      = flag.String("out", "", "directory of the run: where -snap writes")
	fSnap     = flag.String("snap", "", "a command run as `<snap> <out>/before.json` when measuring starts and `<snap> <out>/after.json` when it ends")
)

// snap runs the snapshot command for one edge of the measured window.
func snap(which string) {
	if *fSnap == "" || *fOut == "" {
		return
	}
	cmd := exec.Command("sh", "-c", *fSnap+" "+*fOut+"/"+which+".json")
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}

// expiry is the lifetime every KV write carries.
func appendExpiry(b []byte) []byte {
	if *fForever {
		return append(b, `,"forever":true`...)
	}
	b = append(b, `,"ttlSeconds":`...)
	return strconv.AppendInt(b, int64(*fTTL), 10)
}

func main() {
	flag.Parse()
	urls := strings.Split(*fURL, ",")
	var m mode
	switch *fMode {
	case "kv":
		m = newKvMode()
	case "lock-cycle":
		m = newLockCycle()
	case "lock-renew":
		m = newLockRenew()
	case "lock-contend":
		m = newContend(1)
	case "sem":
		m = newContend(*fLimit)
	case "guard":
		m = newGuardMode()
	case "txn":
		m = newTxnMode()
	default:
		fmt.Fprintln(os.Stderr, "unknown -mode", *fMode)
		os.Exit(2)
	}
	st := newStats(m.series(), m.counters())
	// Far in the future until the set-up is done: nothing is measured before.
	st.measureFrom = time.Now().Add(1000 * time.Hour)
	st.stopAt = st.measureFrom

	fmt.Printf("kvlockload mode=%s tag=%s url=%s workers=%d rate=%.0f duration=%ds warmup=%ds %s\n",
		*fMode, *fTag, *fURL, *fWorkers, *fRate, *fDur, *fWarm, m.describe())

	callers := make([]*caller, *fWorkers)
	for w := range callers {
		host := strings.TrimSuffix(strings.TrimPrefix(urls[w%len(urls)], "http://"), "/")
		callers[w] = &caller{host: host, w: w, st: st}
	}

	// Set-up (locks to hold, for instance), all workers in parallel.
	t := time.Now()
	parallel(*fWorkers, func(w int) { m.setup(callers[w]) })
	if d := time.Since(t); d > 200*time.Millisecond {
		fmt.Printf("[setup] %.1fs\n", d.Seconds())
	}

	start := time.Now()
	st.measureFrom = start.Add(time.Duration(*fWarm) * time.Second)
	st.stopAt = st.measureFrom.Add(time.Duration(*fDur) * time.Second)
	var pc *pacer
	if *fRate > 0 {
		pc = &pacer{start: start, perNs: 1e9 / *fRate}
	}

	var ru0 syscall.Rusage
	go func() {
		time.Sleep(time.Until(st.measureFrom))
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
		snap("before")
	}()
	done := make(chan struct{})
	go reporter(st, m, done)

	parallel(*fWorkers, func(w int) { m.run(callers[w], pc) })
	end := time.Now()
	var ru1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	snap("after")
	close(done)

	elapsed := end.Sub(st.measureFrom).Seconds()
	if end.After(st.stopAt) {
		elapsed = st.stopAt.Sub(st.measureFrom).Seconds()
	}
	summary(st, m, elapsed, cpuSeconds(ru1)-cpuSeconds(ru0), end.Sub(st.measureFrom).Seconds())

	parallel(*fWorkers, func(w int) { m.teardown(callers[w]) })
}

func cpuSeconds(r syscall.Rusage) float64 {
	return float64(r.Utime.Sec) + float64(r.Utime.Usec)/1e6 + float64(r.Stime.Sec) + float64(r.Stime.Usec)/1e6
}

func parallel(n int, f func(w int)) {
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(w int) { defer wg.Done(); f(w) }(w)
	}
	wg.Wait()
}

type mode interface {
	describe() string
	series() []string
	counters() []string
	// The series and the counter whose rate is the headline.
	headline() (seriesName string, opsCounter string)
	setup(c *caller)
	run(c *caller, pc *pacer)
	teardown(c *caller)
}

func reporter(st *stats, m mode, done chan struct{}) {
	tick := time.NewTicker(time.Duration(*fReport) * time.Second)
	defer tick.Stop()
	head, ops := m.headline()
	var lastOps int64
	last := time.Now()
	for {
		select {
		case <-done:
			return
		case now := <-tick.C:
			s := st.series[head]
			cur := s.h.snapshot()
			diff := make([]uint64, histSize)
			for i := range cur {
				diff[i] = cur[i] - s.last[i]
			}
			s.last = cur
			o := st.ctr[ops].Load()
			secs := now.Sub(last).Seconds()
			phase := "run "
			if now.Before(st.measureFrom) {
				phase = "warm"
			}
			fmt.Printf("[%s %s] %s=%8.0f/s calls=%8.0f/s | p50=%7.2f p99=%8.2f ms | errs=%d\n",
				now.Format("15:04:05"), phase, ops, float64(o-lastOps)/secs, float64(countOf(diff))/secs,
				pctOf(diff, 0.50), pctOf(diff, 0.99), st.errTotal.Load())
			lastOps, last = o, now
		}
	}
}

type seriesOut struct {
	Count  uint64  `json:"count"`
	PerSec float64 `json:"perSec"`
	MeanMs float64 `json:"meanMs"`
	P50    float64 `json:"p50"`
	P90    float64 `json:"p90"`
	P99    float64 `json:"p99"`
	P999   float64 `json:"p999"`
	MaxMs  float64 `json:"maxMs"`
}

func summary(st *stats, m mode, elapsed, cpu, wall float64) {
	out := map[string]any{
		"mode": *fMode, "tag": *fTag, "workers": *fWorkers, "rate": *fRate, "seconds": elapsed,
		"batch": *fBatch, "desc": m.describe(), "urls": len(strings.Split(*fURL, ",")),
	}
	ser := map[string]seriesOut{}
	fmt.Printf("\n[final] %.0fs measured\n", elapsed)
	for _, name := range st.order {
		s := st.series[name]
		c := s.h.snapshot()
		n := countOf(c)
		o := seriesOut{Count: n, PerSec: float64(n) / elapsed, P50: pctOf(c, 0.50), P90: pctOf(c, 0.90),
			P99: pctOf(c, 0.99), P999: pctOf(c, 0.999), MaxMs: float64(s.h.max.Load()) / 1000.0}
		if n > 0 {
			o.MeanMs = float64(s.h.total()) / float64(n) / 1000.0
		}
		ser[name] = o
		fmt.Printf("[final] %-10s %9.0f/s  p50=%.2f p90=%.2f p99=%.2f p999=%.2f max=%.1f ms  (n=%d)\n",
			name, o.PerSec, o.P50, o.P90, o.P99, o.P999, o.MaxMs, n)
	}
	ctr := map[string]float64{}
	for _, name := range st.ctrOrder {
		v := st.ctr[name].Load()
		ctr[name] = float64(v)
		fmt.Printf("[final] %-12s %12d  %10.0f/s\n", name, v, float64(v)/elapsed)
	}
	errs := map[string]int64{}
	st.errs.Range(func(k, v any) bool {
		errs[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	kinds := make([]string, 0, len(errs))
	for k := range errs {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		fmt.Printf("[final] err %-40s %d\n", k, errs[k])
	}
	_, ops := m.headline()
	out["series"] = ser
	out["counters"] = ctr
	out["errors"] = errs
	out["errTotal"] = st.errTotal.Load()
	out["opsPerSec"] = ctr[ops] / elapsed
	out["loaderCores"] = cpu / wall
	out["maxLagMs"] = float64(st.maxLagUs.Load()) / 1000.0
	fmt.Printf("[final] loader cpu %.2f cores, errors %d, max schedule lag %.1f ms\n",
		cpu/wall, st.errTotal.Load(), float64(st.maxLagUs.Load())/1000.0)
	b, _ := json.Marshal(out)
	fmt.Printf("[json] %s\n", b)
}

// ---------------------------------------------------------------------------
// kv
// ---------------------------------------------------------------------------

type kvMode struct {
	total int
	value string
}

func newKvMode() *kvMode {
	t := *fIncr + *fGet + *fPia + *fPut + *fDel
	if t <= 0 {
		fmt.Fprintln(os.Stderr, "kv: the weights sum to zero")
		os.Exit(2)
	}
	return &kvMode{total: t, value: strings.Repeat("x", *fValBytes)}
}

func (m *kvMode) describe() string {
	return fmt.Sprintf("ns=%d keys=%d batch=%d mix incr/get/pia/put/del=%d/%d/%d/%d/%d ttl=%ds forever=%v value=%dB",
		*fNS, *fKeys, *fBatch, *fIncr, *fGet, *fPia, *fPut, *fDel, *fTTL, *fForever, *fValBytes)
}
func (m *kvMode) series() []string           { return []string{"call"} }
func (m *kvMode) counters() []string         { return []string{"ops", "applied", "notApplied"} }
func (m *kvMode) headline() (string, string) { return "call", "ops" }
func (m *kvMode) setup(c *caller)            {}
func (m *kvMode) teardown(c *caller)         {}

func appendPadded(b []byte, n, width int) []byte {
	s := strconv.Itoa(n)
	for i := len(s); i < width; i++ {
		b = append(b, '0')
	}
	return append(b, s...)
}

var (
	appliedTrue  = []byte(`"applied":true`)
	appliedFalse = []byte(`"applied":false`)
)

func (m *kvMode) run(c *caller, pc *pacer) {
	rnd := rand.New(rand.NewSource(int64(c.w)*7919 + 13))
	body := make([]byte, 0, 256**fBatch)
	seen := make(map[int]struct{}, *fBatch)
	for {
		t0, ok := begin(pc, c.st)
		if !ok {
			return
		}
		body = append(body[:0], '[')
		clear(seen)
		for j := 0; j < *fBatch; j++ {
			// A key is written at most once per call: draw another on a clash.
			ns, key := rnd.Intn(*fNS), rnd.Intn(*fKeys)
			for {
				if _, dup := seen[ns**fKeys+key]; !dup {
					break
				}
				ns, key = rnd.Intn(*fNS), rnd.Intn(*fKeys)
			}
			seen[ns**fKeys+key] = struct{}{}
			if j > 0 {
				body = append(body, ',')
			}
			r := rnd.Intn(m.total)
			var op, prefix string
			switch {
			case r < *fIncr:
				op, prefix = "incr", "k"
			case r < *fIncr+*fGet:
				op, prefix = "get", "k"
			case r < *fIncr+*fGet+*fPia:
				op, prefix = "putIfAbsent", "idem:k"
			case r < *fIncr+*fGet+*fPia+*fPut:
				op, prefix = "put", "v:k"
			default:
				op, prefix = "delete", "v:k"
			}
			body = append(body, `{"op":"`...)
			body = append(body, op...)
			body = append(body, `","ns":"bench`...)
			body = appendPadded(body, ns, 3)
			body = append(body, `","key":"`...)
			body = append(body, prefix...)
			body = appendPadded(body, key, 6)
			body = append(body, '"')
			switch op {
			case "incr":
				body = appendExpiry(append(body, `,"delta":1`...))
			case "putIfAbsent":
				body = append(body, `,"value":{"w":`...)
				body = strconv.AppendInt(body, int64(c.w), 10)
				body = appendExpiry(append(body, '}'))
			case "put":
				body = append(body, `,"value":"`...)
				body = append(body, m.value...)
				body = appendExpiry(append(body, '"'))
			}
			body = append(body, '}')
		}
		body = append(body, ']')
		resp, ok := c.post("/api/v1/kv", body, t0)
		if !ok {
			continue
		}
		c.st.rec("call", c.w, t0, time.Since(t0))
		c.st.add("ops", t0, int64(*fBatch))
		c.st.add("applied", t0, int64(bytes.Count(resp, appliedTrue)))
		c.st.add("notApplied", t0, int64(bytes.Count(resp, appliedFalse)))
	}
}

// ---------------------------------------------------------------------------
// Locks: the wire
// ---------------------------------------------------------------------------

type lockRes struct {
	Op       string          `json:"op"`
	Name     string          `json:"name"`
	Acquired *bool           `json:"acquired"`
	Renewed  *bool           `json:"renewed"`
	Released *bool           `json:"released"`
	Already  bool            `json:"already"`
	Slot     int             `json:"slot"`
	Token    uint64          `json:"token"`
	Reason   string          `json:"reason"`
	Guard    json.RawMessage `json:"guard"`
}

type lockResp struct {
	Results []lockRes `json:"results"`
}

func yes(b *bool) bool { return b != nil && *b }

func appendAcquire(b []byte, name, owner string, ttl, limit int) []byte {
	b = append(b, `{"op":"acquire","name":"`...)
	b = append(b, name...)
	b = append(b, `","ttlSeconds":`...)
	b = strconv.AppendInt(b, int64(ttl), 10)
	b = append(b, `,"owner":"`...)
	b = append(b, owner...)
	b = append(b, '"')
	if limit > 1 {
		b = append(b, `,"limit":`...)
		b = strconv.AppendInt(b, int64(limit), 10)
	}
	return append(b, '}')
}

func appendRenew(b []byte, name, owner string, slot int, token uint64, ttl int) []byte {
	b = append(b, `{"op":"renew","name":"`...)
	b = append(b, name...)
	b = append(b, `","slot":`...)
	b = strconv.AppendInt(b, int64(slot), 10)
	b = append(b, `,"token":`...)
	b = strconv.AppendUint(b, token, 10)
	b = append(b, `,"ttlSeconds":`...)
	b = strconv.AppendInt(b, int64(ttl), 10)
	b = append(b, `,"owner":"`...)
	b = append(b, owner...)
	return append(b, `"}`...)
}

func appendRelease(b []byte, name string, slot int, token uint64) []byte {
	b = append(b, `{"op":"release","name":"`...)
	b = append(b, name...)
	b = append(b, `","slot":`...)
	b = strconv.AppendInt(b, int64(slot), 10)
	b = append(b, `,"token":`...)
	b = strconv.AppendUint(b, token, 10)
	return append(b, '}')
}

// locks sends one call and decodes it. nil: the call failed (counted).
func (c *caller) locks(body []byte, t0 time.Time, want int) []lockRes {
	raw, ok := c.post("/api/v1/locks", body, t0)
	if !ok {
		return nil
	}
	var r lockResp
	if json.Unmarshal(raw, &r) != nil || len(r.Results) != want {
		c.st.fail("locks_bad_answer", t0)
		return nil
	}
	return r.Results
}

// ---------------------------------------------------------------------------
// lock-cycle: acquire then release, nobody else on the lock
// ---------------------------------------------------------------------------

type lockCycle struct{}

func newLockCycle() *lockCycle { return &lockCycle{} }

func (m *lockCycle) describe() string {
	return fmt.Sprintf("locks per call=%d ttl=%ds", *fBatch, *fTTL)
}
func (m *lockCycle) series() []string { return []string{"cycle", "acquire", "release"} }
func (m *lockCycle) counters() []string {
	return []string{"lockOps", "acquired", "refused", "released", "lost"}
}
func (m *lockCycle) headline() (string, string) { return "cycle", "lockOps" }
func (m *lockCycle) setup(c *caller)            {}
func (m *lockCycle) teardown(c *caller)         {}

func (m *lockCycle) run(c *caller, pc *pacer) {
	n := *fBatch
	owner := "w" + strconv.Itoa(c.w)
	names := make([]string, n)
	for j := range names {
		names[j] = fmt.Sprintf("lc-%d-%d", c.w, j)
	}
	tokens := make([]uint64, n)
	body := make([]byte, 0, 160*n)
	for {
		t0, ok := begin(pc, c.st)
		if !ok {
			return
		}
		body = append(body[:0], '[')
		for j, name := range names {
			if j > 0 {
				body = append(body, ',')
			}
			body = appendAcquire(body, name, owner, *fTTL, 1)
		}
		body = append(body, ']')
		res := c.locks(body, t0, n)
		if res == nil {
			continue
		}
		t1 := time.Now()
		c.st.rec("acquire", c.w, t0, t1.Sub(t0))
		got := 0
		for j, r := range res {
			tokens[j] = 0
			if yes(r.Acquired) {
				tokens[j] = r.Token
				got++
			}
		}
		c.st.add("acquired", t0, int64(got))
		c.st.add("refused", t0, int64(n-got))
		c.st.add("lockOps", t0, int64(n))
		if got == 0 {
			continue
		}
		body = append(body[:0], '[')
		first := true
		for j, name := range names {
			if tokens[j] == 0 {
				continue
			}
			if !first {
				body = append(body, ',')
			}
			first = false
			body = appendRelease(body, name, 0, tokens[j])
		}
		body = append(body, ']')
		res = c.locks(body, t0, got)
		if res == nil {
			continue
		}
		t2 := time.Now()
		c.st.rec("release", c.w, t0, t2.Sub(t1))
		c.st.rec("cycle", c.w, t0, t2.Sub(t0))
		rel := 0
		for _, r := range res {
			if yes(r.Released) {
				rel++
			}
		}
		c.st.add("released", t0, int64(rel))
		c.st.add("lost", t0, int64(got-rel))
		c.st.add("lockOps", t0, int64(got))
	}
}

// ---------------------------------------------------------------------------
// lock-renew: a fleet of holders keeping their locks
// ---------------------------------------------------------------------------

type heldLock struct {
	name  string
	token uint64
}

type lockRenew struct {
	held [][]heldLock
}

func newLockRenew() *lockRenew { return &lockRenew{held: make([][]heldLock, *fWorkers)} }

func (m *lockRenew) describe() string {
	return fmt.Sprintf("held=%d locks per call=%d ttl=%ds", *fHeld, *fBatch, *fTTL)
}
func (m *lockRenew) series() []string           { return []string{"call"} }
func (m *lockRenew) counters() []string         { return []string{"renewOps", "renewed", "lost"} }
func (m *lockRenew) headline() (string, string) { return "call", "renewOps" }

func (m *lockRenew) setup(c *caller) {
	per := *fHeld / *fWorkers
	if per < *fBatch {
		per = *fBatch
	}
	owner := "w" + strconv.Itoa(c.w)
	locks := make([]heldLock, per)
	for j := range locks {
		locks[j].name = fmt.Sprintf("lr-%d-%d", c.w, j)
	}
	var body []byte
	t0 := time.Now()
	for at := 0; at < per; at += 64 {
		end := at + 64
		if end > per {
			end = per
		}
		body = append(body[:0], '[')
		for j := at; j < end; j++ {
			if j > at {
				body = append(body, ',')
			}
			body = appendAcquire(body, locks[j].name, owner, *fTTL, 1)
		}
		body = append(body, ']')
		res := c.locks(body, t0, end-at)
		for i := 0; res == nil && i < 20; i++ {
			time.Sleep(100 * time.Millisecond)
			res = c.locks(body, t0, end-at)
		}
		if res == nil {
			fmt.Fprintln(os.Stderr, "lock-renew: the set-up could not take its locks")
			os.Exit(1)
		}
		for j, r := range res {
			if !yes(r.Acquired) {
				fmt.Fprintf(os.Stderr, "lock-renew: %s is held by somebody else (%s)\n", r.Name, r.Reason)
				os.Exit(1)
			}
			locks[at+j].token = r.Token
		}
	}
	m.held[c.w] = locks
}

func (m *lockRenew) teardown(c *caller) {
	locks := m.held[c.w]
	var body []byte
	t0 := time.Now()
	for at := 0; at < len(locks); at += 64 {
		end := at + 64
		if end > len(locks) {
			end = len(locks)
		}
		body = append(body[:0], '[')
		for j := at; j < end; j++ {
			if j > at {
				body = append(body, ',')
			}
			body = appendRelease(body, locks[j].name, 0, locks[j].token)
		}
		body = append(body, ']')
		c.locks(body, t0, end-at)
	}
}

func (m *lockRenew) run(c *caller, pc *pacer) {
	locks := m.held[c.w]
	owner := "w" + strconv.Itoa(c.w)
	n := *fBatch
	body := make([]byte, 0, 200*n)
	at := 0
	for {
		t0, ok := begin(pc, c.st)
		if !ok {
			return
		}
		body = append(body[:0], '[')
		for j := 0; j < n; j++ {
			l := &locks[(at+j)%len(locks)]
			if j > 0 {
				body = append(body, ',')
			}
			body = appendRenew(body, l.name, owner, 0, l.token, *fTTL)
		}
		body = append(body, ']')
		res := c.locks(body, t0, n)
		if res == nil {
			continue
		}
		c.st.rec("call", c.w, t0, time.Since(t0))
		ren := 0
		for j, r := range res {
			if yes(r.Renewed) {
				locks[(at+j)%len(locks)].token = r.Token
				ren++
			}
		}
		at = (at + n) % len(locks)
		c.st.add("renewed", t0, int64(ren))
		c.st.add("lost", t0, int64(n-ren))
		c.st.add("renewOps", t0, int64(n))
	}
}

// ---------------------------------------------------------------------------
// lock-contend and sem: many workers on few names
// ---------------------------------------------------------------------------

type contend struct {
	limit   int
	holders []atomic.Int32  // per name: permits held now
	slots   []atomic.Int32  // per name and slot: holders now
	top     []atomic.Uint64 // per name and slot: the highest token seen
}

func newContend(limit int) *contend {
	return &contend{
		limit:   limit,
		holders: make([]atomic.Int32, *fLocks),
		slots:   make([]atomic.Int32, *fLocks*limit),
		top:     make([]atomic.Uint64, *fLocks*limit),
	}
}

func (m *contend) describe() string {
	return fmt.Sprintf("names=%d permits=%d hold=%.1fms poll=%s ttl=%ds", *fLocks, m.limit, *fHoldMs, *fPoll, *fTTL)
}
func (m *contend) series() []string { return []string{"wait", "try", "release"} }
func (m *contend) counters() []string {
	return []string{"lockOps", "acquired", "tries", "refused", "contended", "released", "lost", "twoHolders", "tokenWentBack"}
}
func (m *contend) headline() (string, string) { return "try", "lockOps" }
func (m *contend) setup(c *caller)            {}
func (m *contend) teardown(c *caller)         {}

func (m *contend) run(c *caller, pc *pacer) {
	rnd := rand.New(rand.NewSource(int64(c.w)*104729 + 7))
	li := c.w % *fLocks
	prefix := "lk"
	if m.limit > 1 {
		prefix = "sm"
	}
	name := fmt.Sprintf("%s-%d", prefix, li)
	owner := "w" + strconv.Itoa(c.w)
	fixed, sdk := 0.0, *fPoll == "sdk"
	if !sdk {
		fixed, _ = strconv.ParseFloat(*fPoll, 64)
	}
	hold := time.Duration(*fHoldMs * float64(time.Millisecond))
	var body []byte
	for {
		t0 := time.Now()
		if !t0.Before(c.st.stopAt) {
			return
		}
		// ---- wait for the permit
		pause := 100.0
		var got *lockRes
		for {
			ta := time.Now()
			if !ta.Before(c.st.stopAt) {
				return
			}
			body = appendAcquire(append(body[:0], '['), name, owner, *fTTL, m.limit)
			body = append(body, ']')
			res := c.locks(body, ta, 1)
			if res == nil {
				continue
			}
			c.st.rec("try", c.w, ta, time.Since(ta))
			c.st.add("tries", ta, 1)
			c.st.add("lockOps", ta, 1)
			if yes(res[0].Acquired) {
				got = &res[0]
				break
			}
			if res[0].Reason == "contended" {
				c.st.add("contended", ta, 1)
			} else {
				c.st.add("refused", ta, 1)
			}
			ms := fixed
			if sdk {
				ms = pause
				pause *= 1.5
				if pause > 1000 {
					pause = 1000
				}
			}
			if ms > 0 {
				time.Sleep(time.Duration(ms * (0.75 + rnd.Float64()*0.25) * float64(time.Millisecond)))
			}
		}
		// ---- the critical section: nobody else may be in it
		t1 := time.Now()
		c.st.rec("wait", c.w, t0, t1.Sub(t0))
		c.st.add("acquired", t0, 1)
		at := li*m.limit + got.Slot
		inSlot, inName := m.slots[at].Add(1), m.holders[li].Add(1)
		if inSlot > 1 || int(inName) > m.limit {
			c.st.add("twoHolders", t0, 1)
		}
		for {
			prev := m.top[at].Load()
			if got.Token <= prev && !got.Already {
				c.st.add("tokenWentBack", t0, 1)
				break
			}
			if got.Token <= prev || m.top[at].CompareAndSwap(prev, got.Token) {
				break
			}
		}
		if hold > 0 {
			time.Sleep(hold)
		}
		m.slots[at].Add(-1)
		m.holders[li].Add(-1)
		// ---- release
		tr := time.Now()
		body = appendRelease(append(body[:0], '['), name, got.Slot, got.Token)
		body = append(body, ']')
		res := c.locks(body, tr, 1)
		if res == nil {
			continue
		}
		c.st.rec("release", c.w, tr, time.Since(tr))
		c.st.add("lockOps", tr, 1)
		if yes(res[0].Released) {
			c.st.add("released", tr, 1)
		} else {
			c.st.add("lost", tr, 1)
		}
	}
}

// ---------------------------------------------------------------------------
// guard: a KV write behind a lock's guard
// ---------------------------------------------------------------------------

type guardMode struct {
	guards []string
	tokens []uint64
	value  string
}

func newGuardMode() *guardMode {
	return &guardMode{guards: make([]string, *fWorkers), tokens: make([]uint64, *fWorkers),
		value: strings.Repeat("x", *fValBytes)}
}

func (m *guardMode) describe() string {
	return fmt.Sprintf("guard=%v value=%dB ttl=%ds", *fGuard, *fValBytes, *fTTL)
}
func (m *guardMode) series() []string           { return []string{"call"} }
func (m *guardMode) counters() []string         { return []string{"writes", "applied", "refusedByGuard"} }
func (m *guardMode) headline() (string, string) { return "call", "writes" }

// holdLock takes the worker's own lock for the whole run and returns its guard.
func holdLock(c *caller, prefix string) (guard string, token uint64) {
	name := fmt.Sprintf("%s-%d", prefix, c.w)
	ttl := *fWarm + *fDur + 600
	body := appendAcquire([]byte{'['}, name, "w"+strconv.Itoa(c.w), ttl, 1)
	body = append(body, ']')
	t0 := time.Now()
	for i := 0; i < 50; i++ {
		res := c.locks(body, t0, 1)
		if res != nil && yes(res[0].Acquired) {
			return string(res[0].Guard), res[0].Token
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintln(os.Stderr, "could not take", name)
	os.Exit(1)
	return "", 0
}

func dropLock(c *caller, prefix string, token uint64) {
	if token == 0 {
		return
	}
	body := appendRelease([]byte{'['}, fmt.Sprintf("%s-%d", prefix, c.w), 0, token)
	c.locks(append(body, ']'), time.Now(), 1)
}

func (m *guardMode) setup(c *caller) {
	if *fGuard {
		m.guards[c.w], m.tokens[c.w] = holdLock(c, "lg")
	}
}
func (m *guardMode) teardown(c *caller) { dropLock(c, "lg", m.tokens[c.w]) }

var kvPrecondition = []byte(`"kv_precondition"`)

func (m *guardMode) run(c *caller, pc *pacer) {
	body := make([]byte, 0, 512+*fValBytes)
	n := 0
	for {
		t0, ok := begin(pc, c.st)
		if !ok {
			return
		}
		body = append(body[:0], '[')
		if *fGuard {
			body = append(body, m.guards[c.w]...)
			body = append(body, ',')
		}
		body = append(body, `{"op":"put","ns":"guardbench","key":"g`...)
		body = strconv.AppendInt(body, int64(c.w), 10)
		body = append(body, '-')
		body = strconv.AppendInt(body, int64(n%1000), 10)
		body = append(body, `","value":"`...)
		body = append(body, m.value...)
		body = append(body, `","ttlSeconds":`...)
		body = strconv.AppendInt(body, int64(*fTTL), 10)
		body = append(body, `}]`...)
		n++
		resp, ok := c.post("/api/v1/kv", body, t0)
		if !ok {
			continue
		}
		c.st.rec("call", c.w, t0, time.Since(t0))
		c.st.add("writes", t0, 1)
		if bytes.Contains(resp, kvPrecondition) {
			c.st.add("refusedByGuard", t0, 1)
		} else {
			c.st.add("applied", t0, 1)
		}
	}
}

// ---------------------------------------------------------------------------
// txn: a transaction that pushes, with a KV put and/or a lock's guard
// ---------------------------------------------------------------------------

type txnMode struct {
	guards []string
	tokens []uint64
	pad    string
}

func newTxnMode() *txnMode {
	return &txnMode{guards: make([]string, *fWorkers), tokens: make([]uint64, *fWorkers),
		pad: strings.Repeat("x", *fValBytes)}
}

func (m *txnMode) describe() string {
	return fmt.Sprintf("push=1 kvput=%v guard=%v payload pad=%dB queue=%s", *fKvPut, *fGuard, *fValBytes, *fQueue)
}
func (m *txnMode) series() []string           { return []string{"call"} }
func (m *txnMode) counters() []string         { return []string{"txns", "committed", "rolledBack"} }
func (m *txnMode) headline() (string, string) { return "call", "txns" }

func (m *txnMode) setup(c *caller) {
	if *fGuard {
		m.guards[c.w], m.tokens[c.w] = holdLock(c, "lt")
	}
}
func (m *txnMode) teardown(c *caller) { dropLock(c, "lt", m.tokens[c.w]) }

var successTrue = []byte(`"success":true`)

func (m *txnMode) run(c *caller, pc *pacer) {
	body := make([]byte, 0, 768+*fValBytes)
	n := 0
	for {
		t0, ok := begin(pc, c.st)
		if !ok {
			return
		}
		body = append(body[:0], `{"operations":[{"type":"push","items":[{"queue":"`...)
		body = append(body, *fQueue...)
		body = append(body, `","partition":"p`...)
		body = strconv.AppendInt(body, int64(c.w), 10)
		body = append(body, `","payload":{"w":`...)
		body = strconv.AppendInt(body, int64(c.w), 10)
		body = append(body, `,"n":`...)
		body = strconv.AppendInt(body, int64(n), 10)
		body = append(body, `,"pad":"`...)
		body = append(body, m.pad...)
		body = append(body, `"}}]}]`...)
		if *fGuard || *fKvPut {
			body = append(body, `,"kv":[`...)
			if *fGuard {
				body = append(body, m.guards[c.w]...)
			}
			if *fKvPut {
				if *fGuard {
					body = append(body, ',')
				}
				body = append(body, `{"op":"put","ns":"txnbench","key":"t`...)
				body = strconv.AppendInt(body, int64(c.w), 10)
				body = append(body, '-')
				body = strconv.AppendInt(body, int64(n%1000), 10)
				body = append(body, `","value":{"n":`...)
				body = strconv.AppendInt(body, int64(n), 10)
				body = append(body, `},"ttlSeconds":`...)
				body = strconv.AppendInt(body, int64(*fTTL), 10)
				body = append(body, '}')
			}
			body = append(body, ']')
		}
		body = append(body, '}')
		n++
		resp, ok := c.post("/api/v1/transaction", body, t0)
		if !ok {
			continue
		}
		c.st.rec("call", c.w, t0, time.Since(t0))
		c.st.add("txns", t0, 1)
		if bytes.Contains(resp, successTrue) {
			c.st.add("committed", t0, 1)
		} else {
			c.st.add("rolledBack", t0, 1)
		}
	}
}
