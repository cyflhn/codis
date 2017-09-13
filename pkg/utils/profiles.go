package utils

import (
	"runtime/pprof"
)

type GoroutineInfo struct {
	routine string
}

type BlockInfo struct {
	block string
}

func (g *GoroutineInfo) Write(p []byte) (int, error) {
	if p != nil && len(p) > 0 {
		g.routine = string(p)
		return len(p), nil
	}
	return 0, nil
}

func GetGoroutingInfo() string {
	gr := &GoroutineInfo{}
	pprof.Lookup("goroutine").WriteTo(gr, 1)
	return gr.routine
}

func (b *BlockInfo) Write(p []byte) (int, error) {
	if p != nil && len(p) > 0 {
		b.block = string(p)
		return len(p), nil
	}
	return 0, nil
}

func GetBlockInfo() string {
	bl := &BlockInfo{}
	pprof.Lookup("block").WriteTo(bl, 1)
	return bl.block
}
