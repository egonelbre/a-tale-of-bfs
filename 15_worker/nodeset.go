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

func (set NodeSet) GetBuckets1(a graph.Node) (x uint32) {
	x = atomic.LoadUint32(&set[a>>bucket_bits])
	return
}

func (set NodeSet) GetBuckets2(a, b graph.Node) (x, y uint32) {
	x = atomic.LoadUint32(&set[a>>bucket_bits])
	y = atomic.LoadUint32(&set[b>>bucket_bits])
	return
}

func (set NodeSet) GetBuckets3(a, b, c graph.Node) (x, y, z uint32) {
	x = atomic.LoadUint32(&set[a>>bucket_bits])
	y = atomic.LoadUint32(&set[b>>bucket_bits])
	z = atomic.LoadUint32(&set[c>>bucket_bits])
	return
}

func (set NodeSet) GetBuckets4(a, b, c, d graph.Node) (x, y, z, w uint32) {
	x = atomic.LoadUint32(&set[a>>bucket_bits])
	y = atomic.LoadUint32(&set[b>>bucket_bits])
	z = atomic.LoadUint32(&set[c>>bucket_bits])
	w = atomic.LoadUint32(&set[d>>bucket_bits])
	return
}

func (set NodeSet) TryAdd(node graph.Node) bool {
	bucket, bit := set.Offset(node)
	addr := &set[bucket]
retry:
	old := atomic.LoadUint32(addr)
	if old&bit != 0 {
		return false
	}
	if atomic.CompareAndSwapUint32(addr, old, old|bit) {
		return true
	}
	goto retry
}

func (set NodeSet) TryAddFrom(old uint32, node graph.Node) bool {
	bucket, bit := set.Offset(node)
	if old&bit != 0 {
		return false
	}
	addr := &set[bucket]
retry:
	if atomic.CompareAndSwapUint32(addr, old, old|bit) {
		return true
	}
	old = atomic.LoadUint32(addr)
	if old&bit != 0 {
		return false
	}
	goto retry
}
