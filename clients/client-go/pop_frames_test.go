package queen

import (
	"context"
	"encoding/binary"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// The framed pop answer (pop_frames.go), against hand-built answers and a
// scripted server. What is at stake: a consumer that sets FramedPop must be
// handed exactly the messages the JSON answer gives it, and one that does not
// set it must send the request every released broker knows.

// testFrame is one stored frame: u32 body_len | flags | id | txn | [producer] | payload.
func testFrame(id byte, txn, payload string, producer string) []byte {
	le := binary.LittleEndian
	flags := byte(0)
	if producer != "" {
		flags = 2
	}
	body := []byte{flags}
	idb := make([]byte, 16)
	idb[0], idb[6], idb[8] = id, 0x70, 0x80
	body = append(body, idb...)
	body = le.AppendUint16(body, uint16(len(txn)))
	body = append(body, txn...)
	if producer != "" {
		body = le.AppendUint16(body, uint16(len(producer)))
		body = append(body, producer...)
	}
	body = append(body, payload...)
	return append(le.AppendUint32(nil, uint32(len(body))), body...)
}

// framedAnswer is a framed pop answer of one claim on partition 7 ("p7") of
// "orders", lease "lease-1": one record at base offset 10 holding frames,
// created at 2023-11-14T22:13:20Z, of which the claim covers start..=end.
func framedAnswer(extra string, start, end uint64, compress bool, frames ...[]byte) []byte {
	le := binary.LittleEndian
	var raw []byte
	for _, f := range frames {
		raw = append(raw, f...)
	}
	codec := byte(0)
	if compress {
		enc, _ := zstd.NewWriter(nil)
		raw, codec = enc.EncodeAll(raw, nil), 1
	}
	text := func(b []byte, s string) []byte { return append(le.AppendUint16(b, uint16(len(s))), s...) }
	b := append([]byte("QPF1"), 1)
	b = le.AppendUint32(b, uint32(len(extra)))
	b = append(b, extra...)
	b = text(text(text(b, "orders"), "lease-1"), "__QUEUE_MODE__")
	b = le.AppendUint32(b, 1)                     // claims
	b = le.AppendUint64(b, 7)                     // partition id
	b = text(b, "p7")                             // partition
	b = le.AppendUint64(b, start)                 // start
	b = le.AppendUint64(b, end)                   // end
	b = le.AppendUint32(b, 3)                     // delivery attempt
	b = le.AppendUint32(b, 1)                     // records
	b = le.AppendUint64(b, 10)                    // base offset
	b = le.AppendUint64(b, 1_700_000_000_000_000) // created at, microseconds
	b = le.AppendUint32(b, uint32(len(frames)))   // count
	b = append(b, codec)
	b = le.AppendUint32(b, uint32(len(raw)))
	return append(b, raw...)
}

func twoFrames() [][]byte {
	return [][]byte{testFrame(1, "t-10", `{"n":10}`, ""), testFrame(2, "t-11", `{"n":11,"s":"x"}`, "svc")}
}

// The JSON answer of the same two messages, as the broker renders it.
const twoMessagesJSON = `{"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":[` +
	`{"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},` +
	`{"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11}` +
	`],"partitionsClaimed":1}`

func TestDecodePopFrames(t *testing.T) {
	for _, compress := range []bool{false, true} {
		got, err := DecodePopFrames(framedAnswer(`{"frames":true}`, 11, 11, compress, twoFrames()...))
		if err != nil {
			t.Fatalf("compress=%v: %v", compress, err)
		}
		if got.Queue != "orders" || got.LeaseID != "lease-1" || got.ConsumerGroup != "__QUEUE_MODE__" || string(got.Extra) != `{"frames":true}` {
			t.Fatalf("header %+v extra %q", got, got.Extra)
		}
		if len(got.Messages) != 1 {
			t.Fatalf("compress=%v: %d messages, want the one inside the claim", compress, len(got.Messages))
		}
		m := got.Messages[0]
		if m.Offset != 11 || m.TransactionID != "t-11" || string(m.Payload) != `{"n":11,"s":"x"}` || m.ProducerSub != "svc" ||
			m.PartitionID != 7 || m.Partition != "p7" || m.DeliveryAttempt != 3 || m.CreatedAtMicros != 1_700_000_000_000_000 {
			t.Fatalf("message %+v payload %q", m, m.Payload)
		}
		if id := m.IDString(); id != "02000000-0000-7000-8000-000000000000" {
			t.Fatalf("id %q", id)
		}
	}
}

func TestDecodePopFramesRejectsDamage(t *testing.T) {
	b := framedAnswer(`{"frames":true}`, 10, 11, false, twoFrames()...)
	if _, err := DecodePopFrames(b[:len(b)-3]); err == nil {
		t.Fatal("a truncated answer decoded")
	}
	if _, err := DecodePopFrames([]byte(twoMessagesJSON)); err == nil {
		t.Fatal("a JSON answer decoded as frames")
	}
	future := append([]byte(nil), b...)
	future[4] = 9
	if _, err := DecodePopFrames(future); err == nil {
		t.Fatal("an answer of an unknown version decoded")
	}
	// A record that declares more frames than it holds.
	short := framedAnswer(`{}`, 10, 12, false, twoFrames()...)
	short[len(short)-len(twoFrames()[0])-len(twoFrames()[1])-9] = 3 // count: 2 -> 3
	if _, err := DecodePopFrames(short); err == nil {
		t.Fatal("a record with fewer frames than its count decoded")
	}
}

func framedClient(t *testing.T, url string) *Queen {
	t.Helper()
	client, err := New(ClientConfig{URL: url, TimeoutMillis: 2000, RetryAttempts: -1, FramedPop: true})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return client
}

func TestPopSendsNoFramesParameterByDefault(t *testing.T) {
	srv := newCaptureServer(t, okJSON(twoMessagesJSON))
	if _, err := newWireClient(t, srv.URL).Queue("orders").Batch(2).Wait(false).Pop(kvCtx(t)); err != nil {
		t.Fatalf("pop: %v", err)
	}
	if q := srv.only(t).RawQuery; strings.Contains(q, "frames") {
		t.Fatalf("the default pop carries a frames parameter: %q", q)
	}
}

// The same two messages through the JSON answer and through the framed one.
func TestFramedPopGivesTheMessagesOfTheJSONAnswer(t *testing.T) {
	jsonSrv := newCaptureServer(t, okJSON(twoMessagesJSON))
	want, err := newWireClient(t, jsonSrv.URL).Queue("orders").Batch(2).Wait(false).Pop(kvCtx(t))
	if err != nil {
		t.Fatalf("JSON pop: %v", err)
	}
	for _, compress := range []bool{false, true} {
		srv := newCaptureServer(t, cannedResponse{status: 200, body: string(framedAnswer(`{"frames":true}`, 10, 11, compress, twoFrames()...))})
		got, err := framedClient(t, srv.URL).Queue("orders").Batch(2).Wait(false).Pop(kvCtx(t))
		if err != nil {
			t.Fatalf("framed pop: %v", err)
		}
		if q := srv.only(t).RawQuery; !strings.Contains(q, "frames=true") {
			t.Fatalf("the framed pop did not ask for frames: %q", q)
		}
		if len(got) != len(want) || len(got) != 2 {
			t.Fatalf("compress=%v: %d messages, the JSON answer gave %d", compress, len(got), len(want))
		}
		for i := range want {
			w, g := *want[i], *got[i]
			w.trace, g.trace = nil, nil
			if !reflect.DeepEqual(w, g) {
				t.Fatalf("compress=%v, message %d:\n framed %+v\n JSON   %+v", compress, i, g, w)
			}
		}
	}
}

// A broker that does not know the parameter answers JSON: read as always.
func TestFramedPopReadsAJSONAnswer(t *testing.T) {
	srv := newCaptureServer(t, okJSON(twoMessagesJSON))
	got, err := framedClient(t, srv.URL).Queue("orders").Batch(2).Wait(false).Pop(kvCtx(t))
	if err != nil || len(got) != 2 || got[1].TransactionID != "t-11" || got[1].Data["s"] != "x" {
		t.Fatalf("pop: %v, %d messages", err, len(got))
	}
}

func TestFramedPopOfNothing(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{status: 204})
	got, err := framedClient(t, srv.URL).Queue("orders").Wait(false).Pop(kvCtx(t))
	if err != nil || len(got) != 0 {
		t.Fatalf("empty pop: %v, %d messages", err, len(got))
	}
}

// The autopilot's choice travels in the framed answer's own keys.
func TestFramedPopCarriesTheAutopilotChoice(t *testing.T) {
	extra := `{"frames":true,"autopilot":{"partitions":4,"batch":50}}`
	srv := newCaptureServer(t, cannedResponse{status: 200, body: string(framedAnswer(extra, 10, 11, false, twoFrames()...))})
	res, err := framedClient(t, srv.URL).Queue("orders").Wait(false).PopResult(kvCtx(t))
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if len(res.Messages) != 2 || res.Autopilot == nil || res.Autopilot.Partitions != 4 || res.Autopilot.Batch != 50 {
		t.Fatalf("messages %d, autopilot %+v", len(res.Messages), res.Autopilot)
	}
}

// The consumer loop on framed answers: the handler gets every message of the
// claim, in order, and the loop stops at its limit.
func TestConsumeWithFramedPop(t *testing.T) {
	srv := newCaptureServer(t, cannedResponse{status: 200, body: string(framedAnswer(`{"frames":true}`, 10, 11, true, twoFrames()...))})
	client := framedClient(t, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var seen []string
	err := client.Queue("orders").Batch(2).Wait(false).Limit(2).
		Consume(ctx, func(ctx context.Context, msg *Message) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, msg.TransactionID+"@"+msg.PartitionID+"/"+msg.LeaseID)
			return nil
		}).
		Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if want := []string{"t-10@7/lease-1", "t-11@7/lease-1"}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("handled %v, want %v", seen, want)
	}
	for _, r := range srv.requests() {
		if strings.Contains(r.Path, "/pop") && !strings.Contains(r.RawQuery, "frames=true") {
			t.Fatalf("a pop of the consumer did not ask for frames: %q", r.RawQuery)
		}
	}
}
