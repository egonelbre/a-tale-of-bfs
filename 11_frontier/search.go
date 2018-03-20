package search

import (
	"runtime"
	"sync/atomic"

	"github.com/egonelbre/a-tale-of-bfs/graph"
	"github.com/egonelbre/async"
	"github.com/shawnsmithdev/zermelo/zuint32"
)

const (
	ReadBlockSize  = 256
	WriteBlockSize = 256
	SentinelNode   = ^graph.Node(0)
)

type Frontier struct {
	Nodes []graph.Node
	Head  uint32
}

func (front *Frontier) NextRead() (low, high uint32) {
	high = atomic.AddUint32(&front.Head, ReadBlockSize)
	low = high - ReadBlockSize
	if high > uint32(len(front.Nodes)) {
		high = uint32(len(front.Nodes))
	}
	return
}

func (front *Frontier) NextWrite() (low, high uint32) {
	high = atomic.AddUint32(&front.Head, WriteBlockSize)
	low = high - WriteBlockSize
	return
}

func (front *Frontier) Write(low, high *uint32, v graph.Node) {
	if *low >= *high {
		*low, *high = front.NextWrite()
	}
	front.Nodes[*low] = v
	*low += 1
}

func process(g *graph.Graph, currentLevel, nextLevel *Frontier, visited NodeSet) {
	writeLow, writeHigh := uint32(0), uint32(0)
	for {
		readLow, readHigh := currentLevel.NextRead()
		if readLow >= readHigh {
			break
		}

		for _, node := range currentLevel.Nodes[readLow:readHigh] {
			neighbors := g.Neighbors(node)
			i := 0

			for ; i < len(neighbors)-3; i += 4 {
				n1, n2, n3, n4 := neighbors[i], neighbors[i+1], neighbors[i+2], neighbors[i+3]
				if visited.TryAdd(n1) {
					nextLevel.Write(&writeLow, &writeHigh, n1)
				}
				if visited.TryAdd(n2) {
					nextLevel.Write(&writeLow, &writeHigh, n2)
				}
				if visited.TryAdd(n3) {
					nextLevel.Write(&writeLow, &writeHigh, n3)
				}
				if visited.TryAdd(n4) {
					nextLevel.Write(&writeLow, &writeHigh, n4)
				}
			}

			for _, n := range neighbors[i:] {
				if visited.TryAdd(n) {
					nextLevel.Write(&writeLow, &writeHigh, n)
				}
			}
		}
	}

	for i := writeLow; i < writeHigh; i += 1 {
		nextLevel.Nodes[i] = SentinelNode
	}
}

func BreadthFirst(g *graph.Graph, source graph.Node, level []int, procs int) {
	if len(level) != g.Order() {
		panic("invalid level length")
	}

	visited := NewNodeSet(g.Order())

	maxSize := g.Order() + WriteBlockSize*procs

	currentLevel := &Frontier{make([]graph.Node, 0, maxSize), 0}
	nextLevel := &Frontier{make([]graph.Node, maxSize, maxSize), 0}

	level[source] = 1
	visited.TryAdd(source)
	currentLevel.Nodes = append(currentLevel.Nodes, source)

	levelNumber := 2

	for len(currentLevel.Nodes) > 0 {
		async.Run(procs, func(i int) {
			runtime.LockOSThread()
			process(g, currentLevel, nextLevel, visited)
		})

		zuint32.SortBYOB(nextLevel.Nodes[:nextLevel.Head], currentLevel.Nodes[:cap(currentLevel.Nodes)])

		for nextLevel.Head > 0 && nextLevel.Nodes[nextLevel.Head-1] == SentinelNode {
			nextLevel.Head--
		}
		for _, neighbor := range nextLevel.Nodes[:nextLevel.Head] {
			level[neighbor] = levelNumber
		}

		levelNumber++
		currentLevel, nextLevel = nextLevel, currentLevel

		currentLevel.Nodes = currentLevel.Nodes[:currentLevel.Head]
		currentLevel.Head = 0

		nextLevel.Nodes = nextLevel.Nodes[:cap(nextLevel.Nodes)]
		nextLevel.Head = 0
	}
}
