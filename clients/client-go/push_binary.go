package queen

// The binary push body: an alternative to the JSON body of POST /api/v1/push.
//
//	"QPB1" | u8 flags | [u32 raw_len, when zstd] | body
//	body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n
//	       | n x ([u16 len, transactionId, when flag 4] | u32 len, payload)
//
// All integers little-endian. flags: 1 = the body is one zstd frame of raw_len
// bytes, 2 = answer with the counts only, 4 = every item carries its
// transaction id (empty = the broker mints it). No trace id. Each payload is
// one JSON document.
//
// With flag 8 (EncodePushFrames) a group is the messages as the broker stores
// them, compressed here once:
//
//	body:  u32 groups | per group: u16 len, queue | u16 len, partition | u32 n | u32 z_len | z
//	z:     one zstd frame of n frames: u32 body_len | u8 0 | u8[16] message id
//	       | u16 len, transactionId | payload
//
// The message id is a version-7 UUID minted by the client; a message without
// its own transaction id uses the id's text. The broker checks every frame and
// keeps the compressed bytes as they are, so it does not compress them again.
//
// A broker that does not know this body answers 400; a client sends it only
// when ClientConfig.BinaryPush is set, so by default a push is the JSON
// request every broker knows.

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

const (
	pushBinaryMagic       = "QPB1"
	pushBinaryZstd   byte = 1
	pushBinaryShort  byte = 2
	pushBinaryTxn    byte = 4
	pushBinaryFrames byte = 8
)

// rawRequestBody is a request body sent as these bytes, not JSON-encoded.
type rawRequestBody []byte

// PushBatch is one partition's messages for EncodePushBinary.
type PushBatch struct {
	Queue     string
	Partition string
	Payloads  [][]byte // each one JSON document
	// TransactionIDs is nil, or one transaction id per payload ("" = the broker mints it).
	TransactionIDs []string
}

// PushCounts is the short answer of a binary push.
type PushCounts struct {
	Count     int `json:"count"`
	Queued    int `json:"queued"`
	Duplicate int `json:"duplicate"`
	Error     int `json:"error"`
}

var (
	pushZstdOnce sync.Once
	pushZstdEnc  *zstd.Encoder
)

func pushZstdEncoder() *zstd.Encoder {
	pushZstdOnce.Do(func() {
		pushZstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	})
	return pushZstdEnc
}

// EncodePushBinary encodes batches as a binary push body. compress: the body is
// zstd-compressed. short: the broker answers with PushCounts instead of one
// object per message.
func EncodePushBinary(batches []PushBatch, compress, short bool) ([]byte, error) {
	size := 4
	withTxn := false
	for i := range batches {
		b := &batches[i]
		if len(b.Queue) > 0xffff || len(b.Partition) > 0xffff {
			return nil, fmt.Errorf("queue or partition name longer than 65535 bytes")
		}
		if b.TransactionIDs != nil {
			if len(b.TransactionIDs) != len(b.Payloads) {
				return nil, fmt.Errorf("%d transaction ids for %d payloads", len(b.TransactionIDs), len(b.Payloads))
			}
			withTxn = true
			for _, t := range b.TransactionIDs {
				if len(t) > 0xffff {
					return nil, fmt.Errorf("transaction id longer than 65535 bytes")
				}
				size += len(t)
			}
		}
		size += 8 + len(b.Queue) + len(b.Partition) + 2*len(b.Payloads)
		for _, p := range b.Payloads {
			size += 4 + len(p)
		}
	}
	var flags byte
	if short {
		flags |= pushBinaryShort
	}
	if withTxn {
		flags |= pushBinaryTxn
	}
	head := len(pushBinaryMagic) + 1
	body := make([]byte, head, head+size)
	copy(body, pushBinaryMagic)
	body[head-1] = flags
	body = binary.LittleEndian.AppendUint32(body, uint32(len(batches)))
	for i := range batches {
		b := &batches[i]
		body = binary.LittleEndian.AppendUint16(body, uint16(len(b.Queue)))
		body = append(body, b.Queue...)
		body = binary.LittleEndian.AppendUint16(body, uint16(len(b.Partition)))
		body = append(body, b.Partition...)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(b.Payloads)))
		for j, p := range b.Payloads {
			if withTxn {
				txn := ""
				if b.TransactionIDs != nil {
					txn = b.TransactionIDs[j]
				}
				body = binary.LittleEndian.AppendUint16(body, uint16(len(txn)))
				body = append(body, txn...)
			}
			body = binary.LittleEndian.AppendUint32(body, uint32(len(p)))
			body = append(body, p...)
		}
	}
	if !compress {
		return body, nil
	}
	raw := body[head:]
	out := make([]byte, 0, head+4+len(raw)/2)
	out = append(out, pushBinaryMagic...)
	out = append(out, flags|pushBinaryZstd)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(raw)))
	return pushZstdEncoder().EncodeAll(raw, out), nil
}

// EncodePushFrames encodes batches as a binary push body whose groups are the
// messages framed as the broker stores them and compressed once, here. short:
// the broker answers with PushCounts instead of one object per message.
func EncodePushFrames(batches []PushBatch, short bool) ([]byte, error) {
	flags := pushBinaryFrames
	if short {
		flags |= pushBinaryShort
	}
	out := make([]byte, 0, 1024)
	out = append(out, pushBinaryMagic...)
	out = append(out, flags)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(batches)))
	var raw []byte
	var text [36]byte
	for i := range batches {
		b := &batches[i]
		if len(b.Queue) > 0xffff || len(b.Partition) > 0xffff {
			return nil, fmt.Errorf("queue or partition name longer than 65535 bytes")
		}
		if b.TransactionIDs != nil && len(b.TransactionIDs) != len(b.Payloads) {
			return nil, fmt.Errorf("%d transaction ids for %d payloads", len(b.TransactionIDs), len(b.Payloads))
		}
		size := 0
		for _, p := range b.Payloads {
			size += 4 + 1 + 16 + 2 + 36 + len(p)
		}
		raw = raw[:0]
		if cap(raw) < size {
			raw = make([]byte, 0, size)
		}
		for j, p := range b.Payloads {
			id, err := uuid.NewV7()
			if err != nil {
				return nil, err
			}
			var txn []byte
			if b.TransactionIDs != nil && b.TransactionIDs[j] != "" {
				if len(b.TransactionIDs[j]) > 0xffff {
					return nil, fmt.Errorf("transaction id longer than 65535 bytes")
				}
				txn = []byte(b.TransactionIDs[j])
			} else {
				encodeUUIDText(text[:], id)
				txn = text[:]
			}
			raw = binary.LittleEndian.AppendUint32(raw, uint32(1+16+2+len(txn)+len(p)))
			raw = append(raw, 0)
			raw = append(raw, id[:]...)
			raw = binary.LittleEndian.AppendUint16(raw, uint16(len(txn)))
			raw = append(raw, txn...)
			raw = append(raw, p...)
		}
		out = binary.LittleEndian.AppendUint16(out, uint16(len(b.Queue)))
		out = append(out, b.Queue...)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(b.Partition)))
		out = append(out, b.Partition...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Payloads)))
		at := len(out)
		out = append(out, 0, 0, 0, 0)
		out = pushZstdEncoder().EncodeAll(raw, out)
		binary.LittleEndian.PutUint32(out[at:], uint32(len(out)-at-4))
	}
	return out, nil
}

// encodeUUIDText writes the 36-byte lower-case hyphenated text of id into dst.
func encodeUUIDText(dst []byte, id uuid.UUID) {
	const hex = "0123456789abcdef"
	p := 0
	for i, c := range id {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			dst[p] = '-'
			p++
		}
		dst[p] = hex[c>>4]
		dst[p+1] = hex[c&0x0f]
		p += 2
	}
}

// ParsePushCounts decodes the short answer of a binary push.
func ParsePushCounts(b []byte) (PushCounts, error) {
	var c PushCounts
	err := json.Unmarshal(b, &c)
	return c, err
}

// binaryPushBody is items as a binary push body, when every item can travel
// in one: no trace id. Consecutive items of one partition share a group, so
// the order of the items is the order on the wire.
func binaryPushBody(items []PushItem, compress bool) ([]byte, bool) {
	var batches []PushBatch
	for i := range items {
		it := &items[i]
		if it.TraceID != "" {
			return nil, false
		}
		payload, err := json.Marshal(it.Payload)
		if err != nil {
			return nil, false
		}
		n := len(batches)
		if n == 0 || batches[n-1].Queue != it.Queue || batches[n-1].Partition != it.Partition {
			batches = append(batches, PushBatch{Queue: it.Queue, Partition: it.Partition, TransactionIDs: []string{}})
			n++
		}
		batches[n-1].Payloads = append(batches[n-1].Payloads, payload)
		batches[n-1].TransactionIDs = append(batches[n-1].TransactionIDs, it.TransactionID)
	}
	if compress {
		// Compressed = the messages framed and compressed here, kept by the broker as they are.
		body, err := EncodePushFrames(batches, false)
		return body, err == nil
	}
	body, err := EncodePushBinary(batches, false, false)
	return body, err == nil
}
