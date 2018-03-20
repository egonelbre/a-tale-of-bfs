package search

import (
	"sync/atomic"

	"github.com/egonelbre/a-tale-of-bfs/graph"
)

const (
	bucket_bits = 5
	bucket_size = 1 << 5
	bucket_mask = bucket_size - 1
)

type NodeSet []uint32

func NewNodeSet(size int) NodeSet {
	return NodeSet(make([]uint32, (size+31)/32))
}

func (set NodeSet) Offset(node graph.Node) (bucket, bit uint32) {
	bucket = uint32(node >> bucket_bits)
	bit = uint32(1 << (node & bucket_mask))
	return bucket, bit
}

func (set NodeSet) Add(node graph.Node) {
	bucket, bit := set.Offset(node)
	addr := &set[bucket]
	for {
		old := atomic.LoadUint32(addr)
		if old&bit != 0 || atomic.CompareAndSwapUint32(addr, old, old|bit) {
			return
		}
	}
}

func (set NodeSet) Contains(node graph.Node) bool {
	bucket, bit := set.Offset(node)
	return atomic.LoadUint32(&set[bucket])&bit != 0
}
