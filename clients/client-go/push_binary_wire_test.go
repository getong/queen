package queen

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// The binary push body (push_binary.go), asserted against a scripted server.
// What is at stake is the default: a client that does not set BinaryPush must
// send the JSON request every released broker knows, byte for byte.

func pushOne(t *testing.T, cfg ClientConfig, answer string) capturedRequest {
	t.Helper()
	srv := newCaptureServer(t, okJSON(answer))
	cfg.URL, cfg.TimeoutMillis, cfg.RetryAttempts = srv.URL, 2000, -1
	client, err := New(cfg)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	res, err := client.Queue("orders").Partition("p1").Push(map[string]int{"a": 1}).Execute(kvCtx(t))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if len(res) != 1 || res[0].Status != "queued" {
		t.Fatalf("push answered %+v, want one queued item", res)
	}
	return srv.only(t)
}

func TestPushIsJSONByDefault(t *testing.T) {
	req := pushOne(t, ClientConfig{}, `[{"status":"queued"}]`)
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type %q, want application/json", got)
	}
	if !bytes.HasPrefix(req.Body, []byte(`{"items":[`)) {
		t.Fatalf("the default push body is not the JSON one: %q", req.Body)
	}
}

func decodeBinaryPush(t *testing.T, raw []byte, withTxn bool) (queue, partition string, payloads [][]byte) {
	t.Helper()
	le := binary.LittleEndian
	if le.Uint32(raw) != 1 {
		t.Fatalf("want one group, got %d", le.Uint32(raw))
	}
	p := 4
	n := int(le.Uint16(raw[p:]))
	queue = string(raw[p+2 : p+2+n])
	p += 2 + n
	n = int(le.Uint16(raw[p:]))
	partition = string(raw[p+2 : p+2+n])
	p += 2 + n
	count := int(le.Uint32(raw[p:]))
	p += 4
	for i := 0; i < count; i++ {
		if withTxn {
			p += 2 + int(le.Uint16(raw[p:]))
		}
		n = int(le.Uint32(raw[p:]))
		payloads = append(payloads, raw[p+4:p+4+n])
		p += 4 + n
	}
	if p != len(raw) {
		t.Fatalf("%d bytes left after the last payload", len(raw)-p)
	}
	return
}

func TestPushBinaryBody(t *testing.T) {
	req := pushOne(t, ClientConfig{BinaryPush: true}, `[{"status":"queued","transaction_id":"t1"}]`)
	if got := req.Header.Get("Content-Type"); got != "application/x-queen-push" {
		t.Fatalf("content type %q", got)
	}
	if !bytes.HasPrefix(req.Body, []byte("QPB1\x04")) {
		t.Fatalf("not a plain binary push body: %q", req.Body[:8])
	}
	q, part, payloads := decodeBinaryPush(t, req.Body[5:], true)
	if q != "orders" || part != "p1" || len(payloads) != 1 || string(payloads[0]) != `{"a":1}` {
		t.Fatalf("decoded %q %q %q", q, part, payloads)
	}
}

func TestPushBinaryBodyCompressed(t *testing.T) {
	req := pushOne(t, ClientConfig{BinaryPush: true, PushCompression: true}, `[{"status":"queued"}]`)
	if !bytes.HasPrefix(req.Body, []byte("QPB1\x08")) {
		t.Fatalf("not a framed binary push body: %q", req.Body[:8])
	}
	le := binary.LittleEndian
	b := req.Body[5:]
	if le.Uint32(b) != 1 {
		t.Fatalf("want one group, got %d", le.Uint32(b))
	}
	p := 4
	n := int(le.Uint16(b[p:]))
	queue := string(b[p+2 : p+2+n])
	p += 2 + n
	n = int(le.Uint16(b[p:]))
	partition := string(b[p+2 : p+2+n])
	p += 2 + n
	count, zLen := le.Uint32(b[p:]), int(le.Uint32(b[p+4:]))
	p += 8
	if queue != "orders" || partition != "p1" || count != 1 || p+zLen != len(b) {
		t.Fatalf("group %q %q count %d, %d bytes after a %d-byte frame", queue, partition, count, len(b)-p, zLen)
	}
	dec, _ := zstd.NewReader(nil)
	raw, err := dec.DecodeAll(b[p:], nil)
	if err != nil {
		t.Fatalf("zstd: %v", err)
	}
	// One frame: u32 body_len | u8 0 | id[16] | u16 len, txn | payload.
	if int(le.Uint32(raw)) != len(raw)-4 || raw[4] != 0 {
		t.Fatalf("frame header %v", raw[:5])
	}
	id := raw[5:21]
	if id[6]>>4 != 7 || id[8]&0xc0 != 0x80 {
		t.Fatalf("message id is not a version-7 UUID: %x", id)
	}
	tl := int(le.Uint16(raw[21:]))
	if txn := string(raw[23 : 23+tl]); len(txn) != 36 {
		t.Fatalf("transaction id %q", txn)
	}
	if payload := string(raw[23+tl:]); payload != `{"a":1}` {
		t.Fatalf("payload %q", payload)
	}
}

func TestPushWithTraceIDStaysJSON(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`[{"status":"queued"}]`))
	client, err := New(ClientConfig{URL: srv.URL, TimeoutMillis: 2000, RetryAttempts: -1, BinaryPush: true})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if _, err := client.Queue("orders").Push(map[string]int{"a": 1}).TraceID("0190aaaa-0000-7000-8000-000000000001").Execute(kvCtx(t)); err != nil {
		t.Fatalf("push: %v", err)
	}
	if body := srv.only(t).Body; !bytes.HasPrefix(body, []byte(`{"items":[`)) {
		t.Fatalf("an item with a trace id must travel as JSON, got %q", body[:8])
	}
}

func TestEncodePushBinaryShortFlag(t *testing.T) {
	b, err := EncodePushBinary([]PushBatch{{Queue: "q", Partition: "p", Payloads: [][]byte{[]byte(`1`)}}}, false, true)
	if err != nil || !bytes.HasPrefix(b, []byte("QPB1\x02")) {
		t.Fatalf("short flag: %v %q", err, b[:6])
	}
	c, err := ParsePushCounts([]byte(`{"count":3,"queued":2,"duplicate":1,"error":0}`))
	if err != nil || c != (PushCounts{Count: 3, Queued: 2, Duplicate: 1}) {
		t.Fatalf("counts: %v %+v", err, c)
	}
}
