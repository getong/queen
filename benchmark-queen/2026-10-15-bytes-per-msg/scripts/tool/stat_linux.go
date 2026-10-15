package main

import "syscall"

func statBlocks(p string) (int64, bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(p, &st); err != nil {
		return 0, false
	}
	return st.Blocks * 512, true
}
