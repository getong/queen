// ephload — load generator for Queen 2.x ephemeral queues
// (POST /api/v1/ephemeral/push, GET /api/v1/ephemeral/pop, POST /api/v1/ephemeral/ack).
//
// Modes:
//
//	pipe  -pairs producers, each with its own consumer parked on a long poll.
//	      A pair has its own partition of one queue (-perqueue: its own queue).
//	      The consumer commits at delivery; with -ack it takes a lease in a
//	      group and acks every pop.
//	rpc   the request/reply of the guide: a requester pushes to the queue
//	      "rpc" and parks on its inbox; -responders pop "rpc" in one group,
//	      push each answer into the inbox the request names, and ack.
//	      -fresh mints a new inbox (a new queue) for every request.
//	parts one queue with -parts partitions: fill them, read the broker's
//	      memory and idle CPU, then push and pop random partitions by name.
//	      A requester parks for -poptimeout; a reply that arrives only when
//	      that runs out is counted ("answered at the deadline").
//
// Closed loop by default (every producer or requester sends its next call when
// the last one is answered). With -rate the load is open loop: calls are
// scheduled at a fixed pace and latency counts from the scheduled time.
//
// Plain HTTP over one keep-alive connection per goroutine, no SDK. The warm-up
// is not measured. Latency is kept in a log-linear histogram (0.2% wide
// buckets), never sampled. Errors are classified, never just counted.
//
// The histogram and the connection are those of
// ../../2026-10-09-kv-locks/kvlockload.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/bits"
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

func (h *hist) add(shard int, us int64) {
	if us < 0 {
		us = 0
	}
	u := uint64(us)
	h.shards[shard%histShards][bucketOf(u)].Add(1)
	for {
		m := h.max.Load()
		if u <= m || h.max.CompareAndSwap(m, u) {
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

type stats struct {
	measureFrom time.Time
	stopAt      time.Time
	series      map[string]*hist
	order       []string
	ctr         map[string]*atomic.Int64
	ctrOrder    []string
	errs        sync.Map
	errTotal    atomic.Int64
	maxLagUs    atomic.Int64
	// -trace: the worst latency of every 100 ms of wall clock, to see when the tail happens.
	trace0   int64 // the first slot, as epoch milliseconds / 100
	pushMax  []atomic.Int64
	pushSlow []atomic.Int64
	pushCnt  []atomic.Int64
	e2eMax   []atomic.Int64
}

// slot is the 100 ms slot of at, or -1 outside the trace.
func (st *stats) slot(at time.Time) int {
	i := int(at.UnixMilli()/100 - st.trace0)
	if st.pushMax == nil || i < 0 || i >= len(st.pushMax) {
		return -1
	}
	return i
}

func raise(a *atomic.Int64, v int64) {
	for {
		m := a.Load()
		if v <= m || a.CompareAndSwap(m, v) {
			return
		}
	}
}

func newStats(seriesNames, counters []string) *stats {
	st := &stats{series: map[string]*hist{}, ctr: map[string]*atomic.Int64{}}
	for _, n := range seriesNames {
		st.series[n] = newHist()
		st.order = append(st.order, n)
	}
	for _, n := range counters {
		st.ctr[n] = new(atomic.Int64)
		st.ctrOrder = append(st.ctrOrder, n)
	}
	return st
}

// A call or a delivery counts when it ENDS inside the measured window.
func (st *stats) measured(t time.Time) bool {
	return !t.Before(st.measureFrom) && t.Before(st.stopAt)
}

func (st *stats) fail(kind string, at time.Time) {
	if !st.measured(at) {
		return
	}
	st.errTotal.Add(1)
	v, _ := st.errs.LoadOrStore(kind, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// A goroutine's own keep-alive connection, spoken by hand: net/http costs the
// loader about 60 us of CPU a call, which on a machine shared with the broker
// is CPU taken from what is being measured.
type caller struct {
	host string
	st   *stats
	conn net.Conn
	br   *bufio.Reader
	req  []byte
	resp []byte
	// hdr is one more header line ("\r\nname: value"); okAlso a second status that counts as an answer.
	hdr    string
	okAlso int
}

func (c *caller) dial() error {
	conn, err := net.DialTimeout("tcp", c.host, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn, c.br = conn, bufio.NewReaderSize(conn, 64<<10)
	return nil
}

func (c *caller) drop() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// roundTrip writes one request and reads its answer into c.resp.
func (c *caller) roundTrip(method, path string, body []byte) (int, error) {
	if c.conn == nil {
		if err := c.dial(); err != nil {
			return 0, err
		}
	}
	c.req = append(c.req[:0], method...)
	c.req = append(c.req, ' ')
	c.req = append(c.req, path...)
	c.req = append(c.req, " HTTP/1.1\r\nHost: "...)
	c.req = append(c.req, c.host...)
	c.req = append(c.req, c.hdr...)
	if body != nil {
		c.req = append(c.req, "\r\nContent-Type: application/json\r\nContent-Length: "...)
		c.req = strconv.AppendInt(c.req, int64(len(body)), 10)
	}
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
	case status == 204 && length < 0 && !chunked:
		// No content, and nothing says how long it is: there is no body.
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

// call returns the answer's bytes, valid until the next call. ok is false when
// the call failed; the failure is already counted. code is the broker's own
// word for a refusal ("queue_full", "rate_limited").
func (c *caller) call(method, path string, body []byte, want int) (resp []byte, code string, ok bool) {
	status, err := c.roundTrip(method, path, body)
	now := time.Now()
	if err != nil {
		c.drop()
		c.st.fail("transport", now)
		time.Sleep(20 * time.Millisecond)
		return nil, "transport", false
	}
	if status != want && (c.okAlso == 0 || status != c.okAlso) {
		var e struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(c.resp, &e)
		code = e.Code
		if code == "" {
			code = e.Error
		}
		c.st.fail(fmt.Sprintf("http_%d:%s", status, code), now)
		return nil, code, false
	}
	return c.resp, "", true
}

// ---------------------------------------------------------------------------
// Pacing
// ---------------------------------------------------------------------------

// One schedule per goroutine: call n is due at start + n*every.
type pacer struct {
	start time.Time
	every time.Duration
	n     int64
}

// begin gives the next call its start time. Closed loop: now. Open loop: its
// scheduled time, slept up to. false: the run is over.
func begin(pc *pacer, st *stats) (time.Time, bool) {
	if pc == nil {
		t := time.Now()
		return t, t.Before(st.stopAt)
	}
	due := pc.start.Add(time.Duration(pc.n) * pc.every)
	pc.n++
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

func newPacer(rate float64, workers, w int, start time.Time) *pacer {
	if rate <= 0 {
		return nil
	}
	every := time.Duration(float64(time.Second) * float64(workers) / rate)
	// Spread the goroutines over one period, so they do not fire together.
	return &pacer{start: start.Add(every * time.Duration(w) / time.Duration(workers)), every: every}
}

// ---------------------------------------------------------------------------
// Reading a pop's answer without building it
// ---------------------------------------------------------------------------

var (
	keyID    = []byte(`{"id":"`)
	keyStamp = []byte(`"payload":{"t":`)
	keyReply = []byte(`"replyTo":"`)
)

// stamps calls f with the "t" of every message in a pop's answer.
func stamps(resp []byte, f func(t int64)) (n int) {
	for {
		i := bytes.Index(resp, keyStamp)
		if i < 0 {
			return
		}
		resp = resp[i+len(keyStamp):]
		var t int64
		j := 0
		for j < len(resp) && resp[j] >= '0' && resp[j] <= '9' {
			t = t*10 + int64(resp[j]-'0')
			j++
		}
		if j == 0 {
			continue
		}
		f(t)
		n++
	}
}

// quoted calls f with every string that follows key, up to its closing quote.
func quoted(resp, key []byte, f func(v []byte)) {
	for {
		i := bytes.Index(resp, key)
		if i < 0 {
			return
		}
		resp = resp[i+len(key):]
		j := bytes.IndexByte(resp, '"')
		if j < 0 {
			return
		}
		f(resp[:j])
		resp = resp[j:]
	}
}

func ackBody(dst []byte, queue, group string, resp []byte) ([]byte, int) {
	dst = append(dst[:0], `{"queue":"`...)
	dst = append(dst, queue...)
	dst = append(dst, `","group":"`...)
	dst = append(dst, group...)
	dst = append(dst, `","acks":[`...)
	n := 0
	quoted(resp, keyID, func(id []byte) {
		if n > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"id":"`...)
		dst = append(dst, id...)
		dst = append(dst, `","status":"completed"}`...)
		n++
	})
	dst = append(dst, "]}"...)
	return dst, n
}

// ---------------------------------------------------------------------------
// pipe
// ---------------------------------------------------------------------------

// padPool, when set, is random letters and digits the payloads are cut from
// (-randpad): text a compressor gains little on, where the default payload is
// one repeated character.
var padPool []byte

// textPool, when set with padPool (-mixpad), is ordinary words: every payload is
// then half random characters and half words, which a compressor shrinks to
// about half, as it does with typical JSON documents.
var textPool []byte

func makeText(n int) []byte {
	words := strings.Fields("order customer payment status pending shipped delivered address street city country " +
		"amount currency total items quantity price product name email phone created updated version " +
		"reservation checkin checkout guest room night rate channel booking confirmed cancelled message " +
		"request response error retry timeout user account session token value true false null id type")
	p := make([]byte, 0, n+16)
	x := uint64(time.Now().UnixNano()) | 1
	for len(p) < n {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		p = append(p, words[x%uint64(len(words))]...)
		p = append(p, ' ')
	}
	return p[:n]
}

func makePool(n int) []byte {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	p := make([]byte, n)
	x := uint64(time.Now().UnixNano()) | 1
	for i := range p {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		p[i] = chars[x%uint64(len(chars))]
	}
	return p
}

type pipeOpts struct {
	pairs, batch, popBatch, size int
	ack, perQueue                bool
	rate                         float64
	tag                          string
	durable                      bool
	tenant                       string
	anyPart                      bool
}

func address(o pipeOpts, w int) (queue, partition string) {
	if o.durable && o.anyPart {
		// One queue per loader process: its consumers then receive only what its
		// own producers sent, so the two timestamps of a message come from one clock.
		return "pipe-" + strings.TrimSuffix(o.tag, "-"), fmt.Sprintf("p-%d", w)
	}
	if o.perQueue {
		return fmt.Sprintf("pipe-%s%d", o.tag, w), "Default"
	}
	return "pipe", fmt.Sprintf("p-%s%d", o.tag, w)
}

func runPipe(host string, o pipeOpts, st *stats, start time.Time, wg *sync.WaitGroup) {
	pad := strings.Repeat("x", max(0, o.size-len(`{"t":1760000000000000000,"p":""}`)))
	for w := 0; w < o.pairs; w++ {
		queue, partition := address(o, w)

		wg.Add(1)
		go func(w int) { // producer
			defer wg.Done()
			c := &caller{host: host, st: st}
			defer c.drop()
			pushPath := "/api/v1/ephemeral/push"
			if o.durable {
				c.okAlso, pushPath = 200, "/api/v1/push"
				if o.tenant != "" {
					c.hdr = "\r\nx-queen-tenant: " + o.tenant
				}
			}
			pc := newPacer(o.rate/float64(o.batch), o.pairs, w, start)
			rng := uint64(w+1)*0x9E3779B97F4A7C15 + uint64(os.Getpid())
			var body []byte
			for {
				t0, ok := begin(pc, st)
				if !ok {
					return
				}
				if o.durable {
					body = append(body[:0], `{"items":[`...)
				} else {
					body = append(body[:0], `{"queue":"`...)
					body = append(body, queue...)
					body = append(body, `","partition":"`...)
					body = append(body, partition...)
					body = append(body, `","messages":[`...)
				}
				for i := 0; i < o.batch; i++ {
					if i > 0 {
						body = append(body, ',')
					}
					if o.durable {
						body = append(body, `{"queue":"`...)
						body = append(body, queue...)
						body = append(body, `","partition":"`...)
						body = append(body, partition...)
						body = append(body, `",`...)
					} else {
						body = append(body, '{')
					}
					body = append(body, `"payload":{"t":`...)
					body = strconv.AppendInt(body, t0.UnixNano(), 10)
					body = append(body, `,"p":"`...)
					if padPool != nil {
						// A different slice of random text for every message.
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						n := len(pad)
						if textPool != nil {
							n /= 2
							toff := int((rng >> 20) % uint64(len(textPool)-len(pad)))
							body = append(body, textPool[toff:toff+len(pad)-n]...)
						}
						off := int(rng % uint64(len(padPool)-len(pad)))
						body = append(body, padPool[off:off+n]...)
					} else {
						body = append(body, pad...)
					}
					body = append(body, `"}}`...)
				}
				body = append(body, "]}"...)
				sent := time.Now()
				_, code, ok := c.call("POST", pushPath, body, 201)
				done := time.Now()
				if !ok {
					// A full queue is the consumer's pace: wait for it.
					if code == "queue_full" {
						time.Sleep(time.Millisecond)
					} else if code != "transport" {
						time.Sleep(10 * time.Millisecond)
					}
					continue
				}
				if st.measured(done) {
					us := done.Sub(sent).Microseconds()
					st.series["push call"].add(w, us)
					st.ctr["pushed"].Add(int64(o.batch))
					st.ctr["push calls"].Add(1)
					if i := st.slot(done); i >= 0 {
						raise(&st.pushMax[i], us)
						st.pushCnt[i].Add(1)
						if us > 20000 {
							st.pushSlow[i].Add(1)
						}
					}
				}
			}
		}(w)

		wg.Add(1)
		go func(w int) { // consumer
			defer wg.Done()
			c := &caller{host: host, st: st}
			defer c.drop()
			path := fmt.Sprintf("/api/v1/ephemeral/pop?queue=%s&partition=%s&batch=%d&wait=true&timeout=500",
				queue, partition, o.popBatch)
			if o.durable {
				// A durable pop of one partition, committed at delivery; an empty one answers 204.
				path = fmt.Sprintf("/api/v1/pop/queue/%s/partition/%s?batch=%d&wait=true&timeout=500&commitOnDelivery=true", queue, partition, o.popBatch)
				if o.anyPart {
					// Any partition of the queue with work: the shape the broker wakes on a push.
					path = fmt.Sprintf("/api/v1/pop/queue/%s?batch=%d&wait=true&timeout=500&commitOnDelivery=true", queue, o.popBatch)
				}
				c.okAlso = 204
				if o.tenant != "" {
					c.hdr = "\r\nx-queen-tenant: " + o.tenant
				}
			} else if o.ack {
				path += "&group=g"
			} else {
				path += "&commitOnDelivery=true"
			}
			var ack []byte
			// The consumer outlives the producers by a moment, to take what is
			// still in flight; only what lands inside the window is counted.
			for time.Now().Before(st.stopAt.Add(300 * time.Millisecond)) {
				resp, _, ok := c.call("GET", path, nil, 200)
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				got := time.Now()
				in := st.measured(got)
				nowNs := got.UnixNano()
				var worst int64
				n := stamps(resp, func(t int64) {
					if in {
						us := (nowNs - t) / 1000
						st.series["push to delivery"].add(w, us)
						if us > worst {
							worst = us
						}
					}
				})
				if i := st.slot(got); in && i >= 0 {
					raise(&st.e2eMax[i], worst)
				}
				if n == 0 {
					continue
				}
				if in {
					st.ctr["delivered"].Add(int64(n))
					st.ctr["pop calls"].Add(1)
				}
				if o.ack {
					ack, _ = ackBody(ack, queue, "g", resp)
					sent := time.Now()
					if _, _, ok := c.call("POST", "/api/v1/ephemeral/ack", ack, 200); ok {
						if done := time.Now(); st.measured(done) {
							st.series["ack call"].add(w, done.Sub(sent).Microseconds())
						}
					}
				}
			}
		}(w)
	}
}

// ---------------------------------------------------------------------------
// rpc
// ---------------------------------------------------------------------------

type rpcOpts struct {
	requesters, responders int
	fresh                  bool
	popTimeout             time.Duration
	rate                   float64
	tag                    string
}

func runRPC(host string, o rpcOpts, st *stats, start time.Time, wg *sync.WaitGroup) {
	for w := 0; w < o.responders; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			pop := &caller{host: host, st: st}
			defer pop.drop()
			path := "/api/v1/ephemeral/pop?queue=rpc" + o.tag + "&group=workers&batch=10&wait=true&timeout=500"
			var ack, body []byte
			var inboxes [][]byte
			for time.Now().Before(st.stopAt.Add(300 * time.Millisecond)) {
				resp, _, ok := pop.call("GET", path, nil, 200)
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				// The answers reuse the connection, so copy what the pop said first.
				var n int
				ack, n = ackBody(ack, "rpc"+o.tag, "workers", resp)
				if n == 0 {
					continue
				}
				inboxes = inboxes[:0]
				quoted(resp, keyReply, func(v []byte) { inboxes = append(inboxes, append([]byte(nil), v...)) })
				for _, inbox := range inboxes {
					body = append(body[:0], `{"queue":"`...)
					body = append(body, inbox...)
					body = append(body, `","messages":[{"payload":{"ok":true}}]}`...)
					pop.call("POST", "/api/v1/ephemeral/push", body, 201)
				}
				pop.call("POST", "/api/v1/ephemeral/ack", ack, 200)
			}
		}(w)
	}

	for w := 0; w < o.requesters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := &caller{host: host, st: st}
			defer c.drop()
			pc := newPacer(o.rate, o.requesters, w, start)
			inbox := fmt.Sprintf("inbox-%s%d", o.tag, w)
			timeout := strconv.FormatInt(o.popTimeout.Milliseconds(), 10)
			var body []byte
			for n := 0; ; n++ {
				t0, ok := begin(pc, st)
				if !ok {
					return
				}
				if o.fresh {
					inbox = fmt.Sprintf("inbox-%s%d-%d", o.tag, w, n)
				}
				body = append(body[:0], `{"queue":"rpc`...)
				body = append(body, o.tag...)
				body = append(body, `","messages":[{"payload":{"replyTo":"`...)
				body = append(body, inbox...)
				body = append(body, `","n":7}}]}`...)
				if _, _, ok := c.call("POST", "/api/v1/ephemeral/push", body, 201); !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				sent := time.Now()
				resp, _, ok := c.call("GET", "/api/v1/ephemeral/pop?queue="+inbox+"&wait=true&timeout="+timeout+"&commitOnDelivery=true", nil, 200)
				done := time.Now()
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if !bytes.Contains(resp, []byte(`"ok":true`)) {
					st.fail("no reply within the pop's timeout", done)
					continue
				}
				// The reply came, but only when the pop's own timeout ran out: the
				// pop slept through the push. Counted for every request that
				// starts in the window, wherever it ends, so a parked requester
				// shows in a short run.
				if st.measured(t0) && done.Sub(sent) >= o.popTimeout {
					st.ctr["answered at the deadline"].Add(1)
				}
				if st.measured(done) {
					st.series["round trip"].add(w, done.Sub(t0).Microseconds())
					st.ctr["round trips"].Add(1)
				}
			}
		}(w)
	}
}

// ---------------------------------------------------------------------------
// parts: how one queue carries many partitions
// ---------------------------------------------------------------------------

// Fill -parts partitions of one queue with one message each (not measured as
// latency, timed as a rate), read what the broker holds and what it burns while
// nobody calls it, then push to and pop from random partitions by name.
func runParts(host, tag string, parts, workers, pid int, duration time.Duration) {
	st := newStats([]string{"push call", "pop call", "pop with no partition"}, nil)
	st.measureFrom, st.stopAt = time.Now(), time.Now().Add(24*time.Hour)
	push := func(c *caller, body []byte, i int) ([]byte, bool) {
		body = append(body[:0], `{"queue":"parts","partition":"p-`...)
		body = append(body, tag...)
		body = strconv.AppendInt(body, int64(i), 10)
		body = append(body, `","messages":[{"payload":{"t":1}}]}`...)
		_, _, ok := c.call("POST", "/api/v1/ephemeral/push", body, 201)
		return body, ok
	}
	rss0 := rssOf(pid)
	var refused atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := &caller{host: host, st: st}
			defer c.drop()
			var body []byte
			for i := w; i < parts; i += workers {
				for ok := false; !ok; {
					if body, ok = push(c, body, i); ok {
						refused.Store(0)
					} else if refused.Add(1) > 2000 {
						// A broker that is gone must not hold the loader for ever.
						fmt.Println("error: 2000 pushes refused in a row during the fill; giving up")
						os.Exit(3)
					}
				}
			}
		}(w)
	}
	wg.Wait()
	fill := time.Since(t0)
	time.Sleep(2 * time.Second)
	rss1 := rssOf(pid)
	c0 := cpuOf(pid)
	idleFrom := time.Now()
	time.Sleep(5 * time.Second)
	idle := (cpuOf(pid) - c0) / 5
	idleTo := time.Now()

	stop := time.Now().Add(duration)
	var calls atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := &caller{host: host, st: st}
			defer c.drop()
			var body []byte
			x := uint64(w)*2654435761 + 1
			for time.Now().Before(stop) {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				i := int(x % uint64(parts))
				a := time.Now()
				var ok bool
				if body, ok = push(c, body, i); !ok {
					continue
				}
				b := time.Now()
				st.series["push call"].add(w, b.Sub(a).Microseconds())
				if _, _, ok := c.call("GET", "/api/v1/ephemeral/pop?queue=parts&partition=p-"+tag+strconv.Itoa(i)+"&batch=10&commitOnDelivery=true", nil, 200); ok {
					st.series["pop call"].add(w, time.Since(b).Microseconds())
					calls.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	c := &caller{host: host, st: st}
	for k := 0; k < 5; k++ {
		a := time.Now()
		if _, _, ok := c.call("GET", "/api/v1/ephemeral/pop?queue=parts&batch=1&commitOnDelivery=true", nil, 200); ok {
			st.series["pop with no partition"].add(0, time.Since(a).Microseconds())
		}
	}
	c.drop()

	fmt.Printf("== %d partitions in one queue, %d connections\n", parts, workers)
	fmt.Printf("  %-26s %.1f s (%.0f partitions/s)\n", "filled in", fill.Seconds(), float64(parts)/fill.Seconds())
	fmt.Printf("  %-26s %.0f MiB before, %.0f MiB after: %.0f bytes a partition\n", "broker RSS", rss0, rss1, (rss1-rss0)*1048576/float64(parts))
	fmt.Printf("  %-26s %.2f\n", "broker cores, nobody calling", idle)
	fmt.Printf("  %-26s %.0f /s\n", "push + pop by name", float64(calls.Load())/duration.Seconds())
	for _, n := range st.order {
		h := st.series[n].snapshot()
		if countOf(h) == 0 {
			continue
		}
		fmt.Printf("  %-26s p50 %.3f  p99 %.3f  p99.9 %.3f  max %.3f ms\n", n, pctOf(h, 0.50), pctOf(h, 0.99), pctOf(h, 0.999), float64(st.series[n].max.Load())/1000)
	}
	fmt.Printf("  %-26s %.0f MiB\n", "broker RSS at the end", rssOf(pid))
	st.errs.Range(func(k, v any) bool { fmt.Printf("  error %-28s %d\n", k, v.(*atomic.Int64).Load()); return true })
	// The windows, for whoever samples the broker from outside.
	ep := func(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }
	line, _ := json.Marshal(map[string]any{"parts": parts, "fill_s": fill.Seconds(), "fill_from": ep(t0), "fill_to": ep(t0.Add(fill)),
		"idle_from": ep(idleFrom), "idle_to": ep(idleTo), "run_from": ep(idleTo), "run_to": ep(stop), "by_name/s": float64(calls.Load()) / duration.Seconds()})
	fmt.Printf("RESULT %s\n", line)
}

// Resident memory of a process in MiB, as `ps` tells it. -1: unknown.
func rssOf(pid int) float64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return -1
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return -1
	}
	return kb / 1024
}

// ---------------------------------------------------------------------------
// CPU
// ---------------------------------------------------------------------------

// CPU seconds a process has used, as `ps` tells it ("12:34.56"). -1: unknown.
func cpuOf(pid int) float64 {
	if pid <= 0 {
		return -1
	}
	out, err := exec.Command("ps", "-o", "time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return -1
	}
	var s float64
	for _, part := range strings.Split(strings.TrimSpace(string(out)), ":") {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return -1
		}
		s = s*60 + v
	}
	return s
}

func ownCPU() float64 {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return -1
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	var (
		host     = flag.String("host", "127.0.0.1:17732", "broker host:port")
		mode     = flag.String("mode", "pipe", "pipe | rpc")
		name     = flag.String("name", "", "the point's name in the result line")
		duration = flag.Duration("duration", 15*time.Second, "measured time")
		warmup   = flag.Duration("warmup", 3*time.Second, "time before it, not measured")
		rate     = flag.Float64("rate", 0, "open loop: messages (pipe) or requests (rpc) a second; 0 = closed loop")
		pid      = flag.Int("pid", 0, "the broker's pid, to read its CPU")
		durable  = flag.Bool("durable", false, "pipe: a durable queue (POST /api/v1/push, GET /api/v1/pop/queue/q/partition/p) where the default is ephemeral")
		tenant   = flag.String("tenant", "", "pipe -durable: the x-queen-tenant header")
		trace    = flag.Bool("trace", false, "pipe: also report the worst push and delivery latency of every 100 ms")
		mixPad   = flag.Bool("mixpad", false, "pipe: payloads half random characters, half ordinary words: they compress to about half, like JSON documents")
		randPad  = flag.Bool("randpad", false, "pipe: payloads of random letters and digits, which compress little, where the default is one repeated character")
		anyPart  = flag.Bool("anypart", false, "pipe -durable: consumers pop any partition of the queue, where the default is each its own partition")
		tag      = flag.String("tag", "", "a prefix for this process's partitions, queues and inboxes, so several loaders do not share them")

		pairs    = flag.Int("pairs", 8, "pipe: producer and consumer pairs")
		batch    = flag.Int("batch", 1, "pipe: messages per push")
		popBatch = flag.Int("popbatch", 100, "pipe: the pop's batch")
		size     = flag.Int("size", 256, "pipe: payload bytes")
		ack      = flag.Bool("ack", false, "pipe: lease and ack, where the default commits at delivery")
		perQueue = flag.Bool("perqueue", false, "pipe: a queue per pair, where the default is a partition per pair")

		requesters = flag.Int("requesters", 8, "rpc: requesters")
		responders = flag.Int("responders", 4, "rpc: responders")
		fresh      = flag.Bool("fresh", false, "rpc: a new inbox for every request")
		parts      = flag.Int("parts", 100000, "parts: partitions of the one queue")
		popTimeout = flag.Duration("poptimeout", 5*time.Second, "rpc: how long a requester parks on its inbox")
	)
	flag.Parse()

	if *randPad || *mixPad {
		padPool = makePool(16 << 20)
	}
	if *mixPad {
		textPool = makeText(4 << 20)
	}
	if *mode == "sample" {
		// 100 payloads as a push would carry them, to measure how they compress.
		x := uint64(7)
		for i := 0; i < 100; i++ {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			n := 224
			var b []byte
			b = append(b, `{"t":1791700000000000000,"p":"`...)
			if textPool != nil {
				toff := int((x >> 20) % uint64(len(textPool)-224))
				b = append(b, textPool[toff:toff+112]...)
				n = 112
			}
			if padPool != nil {
				off := int(x % uint64(len(padPool)-224))
				b = append(b, padPool[off:off+n]...)
			} else {
				b = append(b, strings.Repeat("x", n)...)
			}
			b = append(b, `"}`...)
			os.Stdout.Write(b)
		}
		return
	}

	if *mode == "parts" {
		runParts(*host, *tag, *parts, *pairs, *pid, *duration)
		return
	}
	if *durable {
		keyStamp = []byte(`"t":`)
	}
	var st *stats
	switch *mode {
	case "pipe":
		st = newStats([]string{"push call", "push to delivery", "ack call"},
			[]string{"pushed", "push calls", "delivered", "pop calls"})
	case "rpc":
		st = newStats([]string{"round trip"}, []string{"round trips", "answered at the deadline"})
	default:
		fmt.Fprintln(os.Stderr, "unknown -mode", *mode)
		os.Exit(2)
	}
	start := time.Now().Add(200 * time.Millisecond)
	st.measureFrom = start.Add(*warmup)
	st.stopAt = st.measureFrom.Add(*duration)

	if *trace {
		n := int(*duration/(100*time.Millisecond)) + 2
		st.trace0 = st.measureFrom.UnixMilli() / 100
		st.pushMax, st.pushSlow = make([]atomic.Int64, n), make([]atomic.Int64, n)
		st.pushCnt, st.e2eMax = make([]atomic.Int64, n), make([]atomic.Int64, n)
	}
	var wg sync.WaitGroup
	switch *mode {
	case "pipe":
		runPipe(*host, pipeOpts{*pairs, *batch, *popBatch, *size, *ack, *perQueue, *rate, *tag, *durable, *tenant, *anyPart}, st, start, &wg)
	case "rpc":
		runRPC(*host, rpcOpts{*requesters, *responders, *fresh, *popTimeout, *rate, *tag}, st, start, &wg)
	}

	time.Sleep(time.Until(st.measureFrom))
	broker0, own0 := cpuOf(*pid), ownCPU()
	time.Sleep(time.Until(st.stopAt))
	broker1, own1 := cpuOf(*pid), ownCPU()
	wg.Wait()

	secs := duration.Seconds()
	res := map[string]any{"name": *name, "mode": *mode, "seconds": secs, "rate": *rate,
		"from": float64(st.measureFrom.UnixNano()) / 1e9, "to": float64(st.stopAt.UnixNano()) / 1e9}
	fmt.Printf("== %s (%s, %.0f s measured)\n", *name, *mode, secs)
	for _, n := range st.ctrOrder {
		v := st.ctr[n].Load()
		res[n] = v
		res[n+"/s"] = float64(v) / secs
		fmt.Printf("  %-18s %12d   %10.0f /s\n", n, v, float64(v)/secs)
	}
	for _, n := range st.order {
		c := st.series[n].snapshot()
		if countOf(c) == 0 {
			continue
		}
		p := map[string]float64{
			"p50": pctOf(c, 0.50), "p90": pctOf(c, 0.90), "p99": pctOf(c, 0.99),
			"p999": pctOf(c, 0.999), "max": float64(st.series[n].max.Load()) / 1000,
		}
		res[n] = p
		fmt.Printf("  %-18s p50 %.3f  p90 %.3f  p99 %.3f  p99.9 %.3f  max %.3f ms\n",
			n, p["p50"], p["p90"], p["p99"], p["p999"], p["max"])
	}
	if broker0 >= 0 && broker1 >= 0 {
		res["broker cores"] = (broker1 - broker0) / secs
		fmt.Printf("  %-18s %.2f\n", "broker cores", (broker1-broker0)/secs)
	}
	res["loader cores"] = (own1 - own0) / secs
	fmt.Printf("  %-18s %.2f\n", "loader cores", (own1-own0)/secs)
	if *rate > 0 {
		res["max lag ms"] = float64(st.maxLagUs.Load()) / 1000
		fmt.Printf("  %-18s %.3f ms\n", "max schedule lag", float64(st.maxLagUs.Load())/1000)
	}
	errs := map[string]int64{}
	st.errs.Range(func(k, v any) bool { errs[k.(string)] = v.(*atomic.Int64).Load(); return true })
	kinds := make([]string, 0, len(errs))
	for k := range errs {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	for _, k := range kinds {
		fmt.Printf("  error %-28s %d\n", k, errs[k])
	}
	res["errors"] = errs
	if st.pushMax != nil {
		ms := func(a []atomic.Int64) []float64 {
			out := make([]float64, len(a))
			for i := range a {
				out[i] = float64(a[i].Load()/100) / 10
			}
			return out
		}
		n := func(a []atomic.Int64) []int64 {
			out := make([]int64, len(a))
			for i := range a {
				out[i] = a[i].Load()
			}
			return out
		}
		res["trace"] = map[string]any{"t0_ms": st.trace0 * 100, "push_max_ms": ms(st.pushMax), "push_over_20ms": n(st.pushSlow),
			"push_calls": n(st.pushCnt), "e2e_max_ms": ms(st.e2eMax)}
	}
	line, _ := json.Marshal(res)
	fmt.Printf("RESULT %s\n", line)
}
