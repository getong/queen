package queen

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// The binary transaction body (push_binary.go), asserted against a scripted
// server. What is at stake is the default (a client that sets neither option
// sends the JSON bundle every released broker knows) and, with the options,
// the order of the results: the broker lists the head's acks before the
// pushes, and the caller must get them back in the order of its own
// operations.

type framedGroup struct {
	queue, partition string
	txns, payloads   []string
}

// readFrameGroups decodes `u32 groups | per group: u16 len, queue | u16 len,
// partition | u32 n | u32 z_len | z` and every frame of every group.
func readFrameGroups(t *testing.T, b []byte) []framedGroup {
	t.Helper()
	le := binary.LittleEndian
	dec, _ := zstd.NewReader(nil)
	n := int(le.Uint32(b))
	p := 4
	groups := make([]framedGroup, 0, n)
	for g := 0; g < n; g++ {
		l := int(le.Uint16(b[p:]))
		queue := string(b[p+2 : p+2+l])
		p += 2 + l
		l = int(le.Uint16(b[p:]))
		partition := string(b[p+2 : p+2+l])
		p += 2 + l
		count, zLen := int(le.Uint32(b[p:])), int(le.Uint32(b[p+4:]))
		p += 8
		raw, err := dec.DecodeAll(b[p:p+zLen], nil)
		if err != nil {
			t.Fatalf("group %d: zstd: %v", g, err)
		}
		declaresItsSize(t, b[p:p+zLen], len(raw))
		p += zLen
		group := framedGroup{queue: queue, partition: partition}
		at := 0
		for i := 0; i < count; i++ {
			// u32 body_len | u8 0 | id[16] | u16 len, txn | payload
			bodyLen := int(le.Uint32(raw[at:]))
			f := raw[at+4 : at+4+bodyLen]
			at += 4 + bodyLen
			if f[0] != 0 {
				t.Fatalf("group %d frame %d carries flags %d", g, i, f[0])
			}
			if id := f[1:17]; id[6]>>4 != 7 || id[8]&0xc0 != 0x80 {
				t.Fatalf("group %d frame %d: the id is not a version-7 UUID: %x", g, i, id)
			}
			tl := int(le.Uint16(f[17:]))
			group.txns = append(group.txns, string(f[19:19+tl]))
			group.payloads = append(group.payloads, string(f[19+tl:]))
		}
		if at != len(raw) {
			t.Fatalf("group %d: %d bytes after its %d frames", g, len(raw)-at, count)
		}
		groups = append(groups, group)
	}
	if p != len(b) {
		t.Fatalf("%d bytes after the last group", len(b)-p)
	}
	return groups
}

func framedTxnClient(t *testing.T, url string) *Queen {
	t.Helper()
	client, err := New(ClientConfig{URL: url, TimeoutMillis: 2000, RetryAttempts: -1, BinaryPush: true, PushCompression: true})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { client.httpClient.Close() })
	return client
}

func TestTransactionIsJSONByDefault(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"ack","success":true},{"index":1,"type":"push","success":true}]}`))
	client := newWireClient(t, srv.URL)
	msg := &Message{TransactionID: "in-1", PartitionID: "7", LeaseID: "lease-1"}
	if _, err := client.Transaction().Ack(msg, AckStatusCompleted, AckOptions{}).
		Queue("out").Partition("p1").Push(map[string]int{"n": 1}).Commit(kvCtx(t)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	req := srv.only(t)
	if len(req.Body) == 0 || req.Body[0] != '{' {
		t.Fatalf("the default transaction body is not JSON: %q", req.Body[:8])
	}
	if ct := req.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
}

func TestTransactionFramedBody(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"ack","success":true,"transactionId":"in-1"},
		{"index":1,"type":"push","success":true,"transactionId":"a"},
		{"index":2,"type":"push","success":true,"transactionId":"tx-b"},
		{"index":3,"type":"push","success":true,"transactionId":"c"}]}`))
	client := framedTxnClient(t, srv.URL)
	msg := &Message{TransactionID: "in-1", PartitionID: "7", LeaseID: "lease-1"}
	res, err := client.Transaction().
		Ack(msg, AckStatusCompleted, AckOptions{}).
		Queue("out").Partition("p1").Push(map[string]int{"n": 1}).
		Queue("out").Partition("p1").Push(PushItem{Payload: map[string]int{"n": 2}, TransactionID: "tx-b"}).
		Queue("out").Partition("p2").Push("text").
		Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !res.Success || len(res.Results) != 4 {
		t.Fatalf("answer %+v", res)
	}

	req := srv.only(t)
	if req.Path != "/api/v1/transaction" {
		t.Fatalf("path = %s", req.Path)
	}
	if !bytes.HasPrefix(req.Body, []byte("QTB1\x00")) {
		t.Fatalf("not a binary transaction body: %q", req.Body[:8])
	}
	headLen := int(binary.LittleEndian.Uint32(req.Body[5:]))
	head := req.Body[9 : 9+headLen]
	// The head: the ack and the lease, and no push.
	assertJSONBody(t, head, `{
		"operations":[{"type":"ack","transactionId":"in-1","partitionId":"7","leaseId":"lease-1","status":"completed"}],
		"requiredLeases":["lease-1"]
	}`)
	// The pushes: consecutive items of one partition share a group.
	groups := readFrameGroups(t, req.Body[9+headLen:])
	if len(groups) != 2 {
		t.Fatalf("want 2 groups, got %+v", groups)
	}
	g := groups[0]
	if g.queue != "out" || g.partition != "p1" || len(g.payloads) != 2 ||
		g.payloads[0] != `{"n":1}` || g.payloads[1] != `{"n":2}` || g.txns[1] != "tx-b" || len(g.txns[0]) != 36 {
		t.Fatalf("first group %+v", g)
	}
	g = groups[1]
	if g.queue != "out" || g.partition != "p2" || len(g.payloads) != 1 || g.payloads[0] != `"text"` {
		t.Fatalf("second group %+v", g)
	}
}

func TestTransactionFramedResultsComeBackInTheCallersOrder(t *testing.T) {
	// The caller pushes, acks, pushes. The broker answers the ack first (it is
	// in the head), then the two pushes, then the rider.
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"ack","success":true,"transactionId":"in-1"},
		{"index":1,"type":"push","success":true,"transactionId":"first"},
		{"index":2,"type":"push","success":true,"transactionId":"second"},
		{"index":3,"type":"kv","opIndex":0,"applied":true}]}`))
	client := framedTxnClient(t, srv.URL)
	msg := &Message{TransactionID: "in-1", PartitionID: "7", LeaseID: "lease-1"}
	res, err := client.Transaction().
		Queue("out").Push(PushItem{Payload: 1, TransactionID: "first"}).
		Ack(msg, AckStatusCompleted, AckOptions{}).
		Queue("out").Push(PushItem{Payload: 2, TransactionID: "second"}).
		Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	type result struct {
		Index         int    `json:"index"`
		Type          string `json:"type"`
		TransactionID string `json:"transactionId"`
	}
	want := []result{{0, "push", "first"}, {1, "ack", "in-1"}, {2, "push", "second"}, {3, "kv", ""}}
	if len(res.Results) != len(want) {
		t.Fatalf("results %s", res.Results)
	}
	for i, w := range want {
		var got result
		if err := json.Unmarshal(res.Results[i], &got); err != nil || got != w {
			t.Errorf("results[%d] = %s, want %+v", i, res.Results[i], w)
		}
	}
	if len(res.KV) != 1 {
		t.Errorf("the rider's result was lost: %+v", res.KV)
	}
	// On the wire the two pushes are one group, in the caller's order.
	req := srv.only(t)
	headLen := int(binary.LittleEndian.Uint32(req.Body[5:]))
	groups := readFrameGroups(t, req.Body[9+headLen:])
	if len(groups) != 1 || len(groups[0].txns) != 2 || groups[0].txns[0] != "first" || groups[0].txns[1] != "second" {
		t.Fatalf("groups %+v", groups)
	}
}

func TestTransactionWithATraceIDStaysJSON(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[{"index":0,"type":"push","success":true}]}`))
	client := framedTxnClient(t, srv.URL)
	if _, err := client.Transaction().Queue("out").
		Push(PushItem{Payload: 1, TraceID: "0190a0a0-0000-7000-8000-000000000001"}).Commit(kvCtx(t)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if req := srv.only(t); len(req.Body) == 0 || req.Body[0] != '{' {
		t.Fatalf("a push with a trace id must travel as JSON: %q", req.Body[:8])
	}
}

func TestTransactionOfAcksOnlyStaysJSON(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[{"index":0,"type":"ack","success":true}]}`))
	client := framedTxnClient(t, srv.URL)
	msg := &Message{TransactionID: "in-1", PartitionID: "7", LeaseID: "lease-1"}
	if _, err := client.Transaction().Ack(msg, AckStatusCompleted, AckOptions{}).Commit(kvCtx(t)); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if req := srv.only(t); len(req.Body) == 0 || req.Body[0] != '{' {
		t.Fatalf("a bundle without pushes has nothing to frame: %q", req.Body[:8])
	}
}

func TestEncodeTransactionFramesShortFlag(t *testing.T) {
	body, err := EncodeTransactionFrames(nil, []PushBatch{{Queue: "q", Partition: "p", Payloads: [][]byte{[]byte(`1`)}}}, true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !bytes.HasPrefix(body, []byte("QTB1\x02\x00\x00\x00\x00")) {
		t.Fatalf("want the short flag and an empty head: %q", body[:9])
	}
	if groups := readFrameGroups(t, body[9:]); len(groups) != 1 || groups[0].payloads[0] != "1" {
		t.Fatalf("groups %+v", groups)
	}
}
