// bpm: bytes-per-message tools for a Queen 2.3.0 data directory.
//
//	bpm sample <blockdev> <datadir> <pid> <out>  once a second, one JSON line: block device writes,
//	                                              process + per-thread write counters, file bytes by kind
//	bpm parse <datadir>                           .qlog composition by record kind and entry content
//	bpm frames <datadir> <every>                  message payloads: what the ids cost after zstd
//
// Formats read here (server/src/rsm): qlog/record.rs (49-byte record header), entry.rs (entry body,
// commands, outcomes), effect.rs (framed effects, payload-free Append), frames.rs (message frames).
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bpm sample|parse|frames ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sample":
		sample(os.Args[2], os.Args[3], os.Args[4], os.Args[5])
	case "parse":
		parse(os.Args[2])
	case "frames":
		every, _ := strconv.Atoi(os.Args[3])
		frames(os.Args[2], every)
	case "lmdb":
		lmdbStat(os.Args[2])
	default:
		fmt.Fprintln(os.Stderr, "unknown command")
		os.Exit(2)
	}
}

// ---------------------------------------------------------------- sample

func readKV(path string) map[string]int64 {
	m := map[string]int64{}
	b, err := os.ReadFile(path)
	if err != nil {
		return m
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			v, _ := strconv.ParseInt(f[1], 10, 64)
			m[strings.TrimSuffix(f[0], ":")] = v
		}
	}
	return m
}

func fileKind(path string) string {
	base := filepath.Base(path)
	switch {
	case strings.HasSuffix(base, ".qlog"):
		return "qlog"
	case strings.HasSuffix(base, ".qidx"):
		return "qidx"
	case base == "data.mdb":
		return "mdb:" + filepath.Base(filepath.Dir(path))
	case base == "lock.mdb":
		return "lockmdb"
	default:
		return "other:" + base
	}
}

type sizes struct {
	Size  int64 `json:"size"`
	Alloc int64 `json:"alloc"`
	N     int   `json:"n"`
}

func walkSizes(dir string) map[string]*sizes {
	out := map[string]*sizes{}
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		k := fileKind(p)
		if strings.HasPrefix(k, "other:") {
			// keep the name class small: strip digits
			k = "other:" + strings.Map(func(r rune) rune {
				if r >= '0' && r <= '9' {
					return -1
				}
				return r
			}, strings.TrimPrefix(k, "other:"))
		}
		s := out[k]
		if s == nil {
			s = &sizes{}
			out[k] = s
		}
		s.Size += info.Size()
		s.N++
		if st, ok := statBlocks(p); ok {
			s.Alloc += st
		}
		return nil
	})
	return out
}

func sample(dev, dir, pid, outPath string) {
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		panic(err)
	}
	w := bufio.NewWriter(f)
	for {
		t := float64(time.Now().UnixNano()) / 1e9
		rec := map[string]any{"t": t}
		if b, err := os.ReadFile("/sys/block/" + dev + "/stat"); err == nil {
			fl := strings.Fields(string(b))
			if len(fl) >= 11 {
				v := func(i int) int64 { x, _ := strconv.ParseInt(fl[i], 10, 64); return x }
				rec["blk"] = map[string]int64{"wios": v(4), "wsec": v(6), "wticks": v(7), "ioticks": v(9)}
			}
		}
		rec["proc"] = readKV("/proc/" + pid + "/io")
		thr := map[string]map[string]int64{}
		tasks, _ := os.ReadDir("/proc/" + pid + "/task")
		for _, t := range tasks {
			base := "/proc/" + pid + "/task/" + t.Name()
			c, err := os.ReadFile(base + "/comm")
			if err != nil {
				continue
			}
			name := strings.TrimSpace(string(c))
			// collapse numbered pools: queen-qlog-writ, tokio-runtime-w ...
			io := readKV(base + "/io")
			m := thr[name]
			if m == nil {
				m = map[string]int64{}
				thr[name] = m
			}
			m["wchar"] += io["wchar"]
			m["wb"] += io["write_bytes"]
			m["syscw"] += io["syscw"]
			m["n"]++
		}
		rec["thr"] = thr
		rec["files"] = walkSizes(dir)
		rec["m"] = scrape()
		b, _ := json.Marshal(rec)
		w.Write(b)
		w.WriteByte('\n')
		w.Flush()
		time.Sleep(time.Second - time.Duration(time.Now().UnixNano()%int64(time.Second)))
	}
}

var scrapeClient = &http.Client{Timeout: 800 * time.Millisecond}

// scrape: the broker's own counters that say how much work the bytes paid for.
func scrape() map[string]float64 {
	out := map[string]float64{}
	resp, err := scrapeClient.Get("http://127.0.0.1:6632/metrics/prometheus")
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	want := []string{
		"queen_process_push_messages_total ", "queen_process_pop_messages_total ", "queen_process_ack_messages_total ",
		`queen_raft_apply_stats{field="entries"}`, `queen_raft_apply_stats{field="appends"}`,
		`queen_raft_apply_stats{field="effects"}`, `queen_raft_apply_stats{field="outcomes_recorded"}`,
		`queen_raft_apply_stats{field="rows_swept"}`,
	}
	for sc.Scan() {
		line := sc.Text()
		for _, w := range want {
			if strings.HasPrefix(line, w) {
				f := strings.Fields(line)
				v, _ := strconv.ParseFloat(f[len(f)-1], 64)
				out[strings.TrimSpace(w)] = v
			}
		}
	}
	return out
}

// ---------------------------------------------------------------- parse

const (
	fileHeaderLen = 32
	recFixed      = 49
	hashLen       = 16
)

type acc struct {
	n     int64
	bytes int64
}

type tally struct {
	cats      map[string]*acc
	messages  int64
	msgRecs   int64
	entryRecs int64
	elidedRecs int64
	stubRecs  int64
	cmdTags   map[uint16]int64
	effKinds  map[uint16]int64
	files     int
	fileBytes int64
	logical   int64
	pushItems int64
	verdicts  map[uint8]int64
	bad       []string
}

func (t *tally) add(cat string, n, b int64) {
	a := t.cats[cat]
	if a == nil {
		a = &acc{}
		t.cats[cat] = a
	}
	a.n += n
	a.bytes += b
}

func u16(b []byte) uint16 { return binary.LittleEndian.Uint16(b) }
func u32(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }
func u64(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }

var tagName = map[uint16]string{0: "empty", 1: "push", 2: "pop", 3: "ack", 4: "renew", 5: "dlqhead", 6: "kv"}
var kindName = map[uint16]string{0: "noop", 1: "queue_upsert", 3: "group_upsert", 5: "partition_create", 7: "append",
	8: "cursor_set", 9: "cursor_delete", 12: "watermark", 13: "kv_put", 14: "kv_delete", 15: "timer_upsert",
	16: "timer_delete", 27: "garbage_add", 28: "delete_chunk", 29: "request_ids_expire", 30: "cluster_version_set",
	31: "membership_note", 33: "trace_record", 34: "trace_trim"}

func name(m map[uint16]string, k uint16) string {
	if s, ok := m[k]; ok {
		return s
	}
	return "k" + strconv.Itoa(int(k))
}

// parseEntry attributes the payload-free entry bytes (entry.rs wire form). An elided record
// (kind 4) keeps the ORIGINAL frame header in front of a body whose Append hash lists are gone.
func parseEntry(t *tally, e []byte, elided bool) {
	if len(e) < 12 {
		t.add("entry/garbled", 1, int64(len(e)))
		return
	}
	bodyLen := int(u32(e))
	body := e[12:]
	if !elided && bodyLen != len(body) {
		t.bad = append(t.bad, fmt.Sprintf("entry body_len %d != %d", bodyLen, len(body)))
		t.add("entry/garbled", 1, int64(len(e)))
		return
	}
	t.add("entry/frame(len+xxh3)", 1, 12)
	p := 0
	// format u16 | kinds_version u32 | now i64 | pid_base u64 | kv_version_base u64
	p += 2 + 4 + 8 + 8 + 8
	ncmd := int(u32(body[p:]))
	p += 4
	t.add("entry/header", 1, int64(p))
	for i := 0; i < ncmd; i++ {
		start := p
		p += 16 + 4 + 4 // request id, first_effect, effect_count
		tag := u16(body[p:])
		olen := int(u32(body[p+4:]))
		p += 8
		t.cmdTags[tag]++
		t.add("cmd/request_id+effect_range", 1, int64(p-8-start))
		t.add("cmd/outcome_header", 1, 8)
		ob := body[p : p+olen]
		cat := "outcome/" + name(tagName, tag)
		if tag == 1 && len(ob) >= 4 {
			n := int(u32(ob))
			t.pushItems += int64(n)
			q := 4
			for j := 0; j < n && q < len(ob); j++ {
				v := ob[q]
				t.verdicts[v]++
				switch v {
				case 0:
					q += 1 + 8 + 8 + 8
				case 1:
					q += 1 + 8 + 8
				default:
					q++
					cl := int(u32(ob[q:]))
					q += 4 + cl
					ml := int(u32(ob[q:]))
					q += 4 + ml
				}
			}
		}
		t.add(cat, 1, int64(olen))
		p += olen
	}
	neff := int(u32(body[p:]))
	p += 4
	t.add("entry/effect_count", 1, 4)
	for i := 0; i < neff; i++ {
		kind := u16(body[p:])
		blen := int(u32(body[p+4:]))
		eb := body[p+8 : p+8+blen]
		t.effKinds[kind]++
		t.add("effect_header/"+name(kindName, kind), 1, 8)
		if kind == 7 {
			// pid u64 | bucket u16 | base u64 | count u32 | created i64 | blob(hashes) | blob(frame_len)
			hl := int(u32(eb[30:]))
			t.add("append/fixed_fields", 1, 30)
			t.add("append/hashes_len_prefix", 1, 4)
			t.add("append/hashes", 1, int64(hl))
			t.add("append/frame_len_blob", 1, int64(blen-30-4-hl))
		} else {
			t.add("effect_body/"+name(kindName, kind), 1, int64(blen))
		}
		p += 8 + blen
	}
	if p != len(body) {
		t.bad = append(t.bad, fmt.Sprintf("entry walk ended at %d of %d", p, len(body)))
	}
}

func qlogFiles(dir string) []string {
	var files []string
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".qlog") {
			files = append(files, p)
		}
		return nil
	})
	sort.Strings(files)
	return files
}

func parse(dir string) {
	t := &tally{cats: map[string]*acc{}, cmdTags: map[uint16]int64{}, effKinds: map[uint16]int64{}, verdicts: map[uint8]int64{}}
	for _, path := range qlogFiles(dir) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.bad = append(t.bad, err.Error())
			continue
		}
		t.files++
		t.fileBytes += int64(len(b))
		if len(b) < fileHeaderLen || string(b[:7]) != "QNQLOG1" {
			t.bad = append(t.bad, path+": no qlog header")
			continue
		}
		t.add("file/header", 1, fileHeaderLen)
		p := fileHeaderLen
		for p+recFixed <= len(b) {
			bodyLen := int(u32(b[p:]))
			if bodyLen < recFixed-4 || p+4+bodyLen > len(b) {
				break // zero run (preallocation) or torn tail
			}
			rec := b[p : p+4+bodyLen]
			count := int64(u32(rec[36:]))
			kind := rec[48] & 0x7f
			zst := rec[48]&0x80 != 0
			switch kind {
			case 0, 1:
				t.msgRecs++
				t.messages += count
				t.add("msgrec/header", 1, recFixed)
				off := recFixed
				if kind == 1 {
					n := int(u16(rec[recFixed+16:]))
					off += 18 + 8*n
					t.add("msgrec/txn_envelope", 1, int64(18+8*n))
				}
				t.add("msgrec/hashes", 1, count*hashLen)
				pl := int64(len(rec) - off - int(count)*hashLen)
				if zst {
					t.add("msgrec/payload_zstd", 1, pl)
				} else {
					t.add("msgrec/payload_raw", 1, pl)
				}
			case 2:
				t.entryRecs++
				t.add("entryrec/header", 1, recFixed)
				parseEntry(t, rec[recFixed:], false)
			case 4:
				t.entryRecs++
				t.elidedRecs++
				t.add("entryrec/header", 1, recFixed)
				parseEntry(t, rec[recFixed:], true)
			case 3:
				t.stubRecs++
				t.add("entryrec/stub", 1, int64(len(rec)))
			default:
				t.bad = append(t.bad, fmt.Sprintf("%s@%d: kind %d", path, p, kind))
			}
			p += len(rec)
		}
		t.logical += int64(p)
		if p < len(b) {
			// what is after the logical end: zeros (preallocation) or a torn tail
			zeros := true
			for _, c := range b[p:] {
				if c != 0 {
					zeros = false
					break
				}
			}
			if zeros {
				t.add("file/zero_tail", 1, int64(len(b)-p))
			} else {
				t.add("file/nonzero_tail", 1, int64(len(b)-p))
			}
		}
	}
	keys := make([]string, 0, len(t.cats))
	for k := range t.cats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return t.cats[keys[i]].bytes > t.cats[keys[j]].bytes })
	fmt.Printf("files %d, file bytes %d, logical %d, message records %d, messages %d, entry records %d (elided %d), stubs %d\n",
		t.files, t.fileBytes, t.logical, t.msgRecs, t.messages, t.entryRecs, t.elidedRecs, t.stubRecs)
	m := float64(t.messages)
	if m == 0 {
		m = 1
	}
	fmt.Printf("%-40s %14s %12s %10s\n", "category", "bytes", "count", "B/msg")
	var total int64
	for _, k := range keys {
		a := t.cats[k]
		total += a.bytes
		fmt.Printf("%-40s %14d %12d %10.2f\n", k, a.bytes, a.n, float64(a.bytes)/m)
	}
	fmt.Printf("%-40s %14d %12s %10.2f\n", "TOTAL", total, "", float64(total)/m)
	fmt.Printf("commands by outcome tag:")
	for k, v := range t.cmdTags {
		fmt.Printf(" %s=%d", name(tagName, k), v)
	}
	fmt.Printf("\neffects by kind:")
	for k, v := range t.effKinds {
		fmt.Printf(" %s=%d", name(kindName, k), v)
	}
	fmt.Printf("\npush items %d, verdicts %v\n", t.pushItems, t.verdicts)
	for i, s := range t.bad {
		if i > 20 {
			fmt.Printf("... %d more\n", len(t.bad)-i)
			break
		}
		fmt.Println("BAD:", s)
	}
}

// ---------------------------------------------------------------- frames

type msgFrame struct {
	flags   byte
	id      []byte
	trace   []byte
	txn     []byte
	psub    []byte
	payload []byte
}

func splitFrames(raw []byte) ([]msgFrame, error) {
	var out []msgFrame
	p := 0
	for p < len(raw) {
		if p+4 > len(raw) {
			return nil, fmt.Errorf("short frame header")
		}
		n := int(u32(raw[p:]))
		body := raw[p+4 : p+4+n]
		q := 0
		f := msgFrame{flags: body[0]}
		q++
		f.id = body[q : q+16]
		q += 16
		if f.flags&1 != 0 {
			f.trace = body[q : q+16]
			q += 16
		}
		tl := int(u16(body[q:]))
		q += 2
		f.txn = body[q : q+tl]
		q += tl
		if f.flags&2 != 0 {
			pl := int(u16(body[q:]))
			q += 2
			f.psub = body[q : q+pl]
			q += pl
		}
		f.payload = body[q:]
		out = append(out, f)
		p += 4 + n
	}
	return out, nil
}

func uuidText(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func packFrames(fr []msgFrame, keepTxn, keepID bool) []byte {
	var out []byte
	for _, f := range fr {
		var body []byte
		body = append(body, f.flags)
		if keepID {
			body = append(body, f.id...)
		}
		if f.trace != nil {
			body = append(body, f.trace...)
		}
		txn := f.txn
		if !keepTxn {
			txn = nil
		}
		body = binary.LittleEndian.AppendUint16(body, uint16(len(txn)))
		body = append(body, txn...)
		if f.psub != nil {
			body = binary.LittleEndian.AppendUint16(body, uint16(len(f.psub)))
			body = append(body, f.psub...)
		}
		body = append(body, f.payload...)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(body)))
		out = append(out, body...)
	}
	return out
}

func frames(dir string, every int) {
	dec, _ := zstd.NewReader(nil)
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithSingleSegment(true))
	var recs, msgs, stored, again, noTxn, noTxnNoID, rawAll, rawJSON, txnIsID, txnLen int64
	i := 0
	for _, path := range qlogFiles(dir) {
		b, err := os.ReadFile(path)
		if err != nil || len(b) < fileHeaderLen {
			continue
		}
		p := fileHeaderLen
		for p+recFixed <= len(b) {
			bodyLen := int(u32(b[p:]))
			if bodyLen < recFixed-4 || p+4+bodyLen > len(b) {
				break
			}
			rec := b[p : p+4+bodyLen]
			p += len(rec)
			kind := rec[48] & 0x7f
			if kind != 0 {
				continue
			}
			i++
			if every > 1 && i%every != 0 {
				continue
			}
			count := int(u32(rec[36:]))
			pl := rec[recFixed+count*hashLen:]
			raw := pl
			if rec[48]&0x80 != 0 {
				raw, err = dec.DecodeAll(pl, nil)
				if err != nil {
					continue
				}
			}
			fr, err := splitFrames(raw)
			if err != nil || len(fr) != count {
				continue
			}
			recs++
			msgs += int64(count)
			stored += int64(len(pl))
			rawAll += int64(len(raw))
			for _, f := range fr {
				rawJSON += int64(len(f.payload))
				txnLen += int64(len(f.txn))
				if string(f.txn) == uuidText(f.id) {
					txnIsID++
				}
			}
			again += int64(len(enc.EncodeAll(packFrames(fr, true, true), nil)))
			noTxn += int64(len(enc.EncodeAll(packFrames(fr, false, true), nil)))
			noTxnNoID += int64(len(enc.EncodeAll(packFrames(fr, false, false), nil)))
		}
	}
	m := float64(msgs)
	fmt.Printf("sampled records %d, messages %d (txn text == id text for %d)\n", recs, msgs, txnIsID)
	fmt.Printf("raw frames %.1f B/msg (JSON payload %.1f, txn text %.1f)\n", float64(rawAll)/m, float64(rawJSON)/m, float64(txnLen)/m)
	fmt.Printf("stored (client zstd) %.2f B/msg | recompressed as is %.2f | without txn text %.2f | without txn text and id %.2f\n",
		float64(stored)/m, float64(again)/m, float64(noTxn)/m, float64(noTxnNoID)/m)
}
