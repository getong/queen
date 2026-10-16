package main

// bpm lmdb <data.mdb>: what each named database (store keyspace) occupies, from the file itself
// (liblmdb mdb.c of lmdb-master-sys 0.2.6: 16-byte page header, 8-byte node header, MDB_db 48 bytes).

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

const (
	pBranch   = 0x01
	pLeaf     = 0x02
	pOverflow = 0x04
	fBigdata  = 0x01
	fSubdata  = 0x02
	pageHdr   = 16
)

type mdbDB struct {
	pad, flags, depth                       uint32
	branch, leaf, overflow, entries, root uint64
}

func readDB(b []byte) mdbDB {
	return mdbDB{
		pad:      binary.LittleEndian.Uint32(b[0:]),
		flags:    uint32(binary.LittleEndian.Uint16(b[4:])),
		depth:    uint32(binary.LittleEndian.Uint16(b[6:])),
		branch:   binary.LittleEndian.Uint64(b[8:]),
		leaf:     binary.LittleEndian.Uint64(b[16:]),
		overflow: binary.LittleEndian.Uint64(b[24:]),
		entries:  binary.LittleEndian.Uint64(b[32:]),
		root:     binary.LittleEndian.Uint64(b[40:]),
	}
}

type walkStat struct {
	branchPages, leafPages, ovPages int64
	nodes, nodeBytes, bigNodes      int64
	keyBytes, valBytes, bigValBytes int64
}

type lmdbFile struct {
	b     []byte
	psize int
}

func (f *lmdbFile) page(pg uint64) []byte {
	off := int(pg) * f.psize
	if off+f.psize > len(f.b) {
		return nil
	}
	return f.b[off : off+f.psize]
}

func (f *lmdbFile) walk(pg uint64, st *walkStat, visit func(key, val []byte, flags uint16)) {
	p := f.page(pg)
	if p == nil {
		return
	}
	flags := binary.LittleEndian.Uint16(p[10:])
	lower := int(binary.LittleEndian.Uint16(p[12:]))
	n := (lower - pageHdr) >> 1
	if flags&pBranch != 0 {
		st.branchPages++
		for i := 0; i < n; i++ {
			off := int(binary.LittleEndian.Uint16(p[pageHdr+2*i:]))
			lo := uint64(binary.LittleEndian.Uint16(p[off:]))
			hi := uint64(binary.LittleEndian.Uint16(p[off+2:]))
			fl := uint64(binary.LittleEndian.Uint16(p[off+4:]))
			child := lo | hi<<16 | fl<<32
			f.walk(child, st, visit)
		}
		return
	}
	if flags&pLeaf == 0 {
		return
	}
	st.leafPages++
	for i := 0; i < n; i++ {
		off := int(binary.LittleEndian.Uint16(p[pageHdr+2*i:]))
		dsize := int(binary.LittleEndian.Uint16(p[off:])) | int(binary.LittleEndian.Uint16(p[off+2:]))<<16
		nf := binary.LittleEndian.Uint16(p[off+4:])
		ks := int(binary.LittleEndian.Uint16(p[off+6:]))
		key := p[off+8 : off+8+ks]
		st.nodes++
		st.keyBytes += int64(ks)
		st.valBytes += int64(dsize)
		if nf&fBigdata != 0 {
			st.bigNodes++
			st.bigValBytes += int64(dsize)
			st.nodeBytes += int64(8 + ks + 8 + 2)
			ov := binary.LittleEndian.Uint64(p[off+8+ks:])
			op := f.page(ov)
			if op != nil {
				st.ovPages += int64(binary.LittleEndian.Uint32(op[12:]))
			}
			if visit != nil {
				visit(key, nil, nf)
			}
		} else {
			st.nodeBytes += int64(8 + ks + dsize + 2)
			if visit != nil {
				visit(key, p[off+8+ks:off+8+ks+dsize], nf)
			}
		}
	}
}

func lmdbStat(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	f := &lmdbFile{b: b, psize: 4096}
	var best []byte
	var bestTxn uint64
	for m := 0; m < 2; m++ {
		meta := b[m*4096+pageHdr:]
		if binary.LittleEndian.Uint32(meta) != 0xBEEFC0DE {
			continue
		}
		// magic 4 | version 4 | address 8 | mapsize 8 | dbs 2x48 | last_pg 8 | txnid 8
		txn := binary.LittleEndian.Uint64(meta[24+96+8:])
		if best == nil || txn > bestTxn {
			best, bestTxn = meta, txn
		}
	}
	if best == nil {
		fmt.Println("no meta page")
		return
	}
	free := readDB(best[24:])
	main := readDB(best[24+48:])
	f.psize = int(free.pad)
	lastPg := binary.LittleEndian.Uint64(best[24+96:])
	fmt.Printf("data.mdb %d bytes, page %d, last_pg %d (%d MB used), txnid %d\n", len(b), f.psize, lastPg, (lastPg+1)*uint64(f.psize)>>20, bestTxn)
	fmt.Printf("free db: entries %d leaf %d branch %d overflow %d\n", free.entries, free.leaf, free.branch, free.overflow)
	// free pages listed in the freeDB: each entry's value is an IDL (count + pgnos)
	var freePages int64
	if free.root != ^uint64(0) {
		var st walkStat
		f.walk(free.root, &st, func(key, val []byte, flags uint16) {
			if val != nil && len(val) >= 8 {
				freePages += int64(binary.LittleEndian.Uint64(val))
			}
		})
	}
	fmt.Printf("free pages listed: %d (%d MB)\n", freePages, freePages*int64(f.psize)>>20)
	type row struct {
		name string
		db   mdbDB
		st   walkStat
	}
	var rows []row
	var mst walkStat
	f.walk(main.root, &mst, func(key, val []byte, flags uint16) {
		if flags&fSubdata != 0 && len(val) >= 48 {
			db := readDB(val)
			var st walkStat
			if db.root != ^uint64(0) {
				f.walk(db.root, &st, nil)
			}
			rows = append(rows, row{string(key), db, st})
		}
	})
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].db.leaf+rows[i].db.overflow+rows[i].db.branch > rows[j].db.leaf+rows[j].db.overflow+rows[j].db.branch
	})
	fmt.Printf("%-18s %10s %6s %9s %9s %9s %9s %8s %8s %8s %8s\n", "keyspace", "entries", "depth", "leaf", "branch", "overflow", "MB", "rows/lf", "fill%", "avgkey", "avgval")
	for _, r := range rows {
		pages := r.db.leaf + r.db.branch + r.db.overflow
		rpl, fill, ak, av := 0.0, 0.0, 0.0, 0.0
		if r.st.leafPages > 0 {
			rpl = float64(r.st.nodes) / float64(r.st.leafPages)
			fill = 100 * float64(r.st.nodeBytes) / float64(r.st.leafPages*int64(f.psize-pageHdr))
		}
		if r.st.nodes > 0 {
			ak = float64(r.st.keyBytes) / float64(r.st.nodes)
			av = float64(r.st.valBytes) / float64(r.st.nodes)
		}
		fmt.Printf("%-18s %10d %6d %9d %9d %9d %9.1f %8.2f %8.1f %8.1f %8.1f\n", r.name, r.db.entries, r.db.depth, r.db.leaf, r.db.branch, r.db.overflow,
			float64(pages*uint64(f.psize))/1e6, rpl, fill, ak, av)
	}
}
