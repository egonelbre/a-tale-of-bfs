package search

import (
	"runtime"
	"sync"
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
			if node == SentinelNode {
				continue
			}

			neighbors := g.Neighbors(node)
			i := 0

			for ; i < len(neighbors)-3; i += 4 {
				n1, n2, n3, n4 := neighbors[i], neighbors[i+1], neighbors[i+2], neighbors[i+3]
				x1, x2, x3, x4 := visited.GetBuckets4(n1, n2, n3, n4)
				if visited.TryAddFrom(x1, n1) {
					nextLevel.Write(&writeLow, &writeHigh, n1)
				}
				if visited.TryAddFrom(x2, n2) {
					nextLevel.Write(&writeLow, &writeHigh, n2)
				}
				if visited.TryAddFrom(x3, n3) {
					nextLevel.Write(&writeLow, &writeHigh, n3)
				}
				if visited.TryAddFrom(x4, n4) {
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

	var waitForLast1, waitForLast2 sync.WaitGroup
	doneProcessingCounter := int32(procs)
	waitForLast1.Add(1)

	allDone := uint32(0)

	worker := func(gid int) {
		runtime.LockOSThread()

		for atomic.LoadUint32(&allDone) == 0 {
			{
				// process the current level in parallel
				process(g, currentLevel, nextLevel, visited)
			}

			// use a counter to see how many are still processing
			if atomic.AddInt32(&doneProcessingCounter, -1) == 0 {
				// the last one updates the Nodes size
				{
					nextLevel.Nodes = nextLevel.Nodes[:nextLevel.Head]
					nextLevel.Head = 0
				}

				// reset counters
				atomic.StoreInt32(&doneProcessingCounter, int32(procs))
				waitForLast2.Add(1)
				// ... and release the routines
				waitForLast1.Done()
			} else {
				// wait for the last one finishing processing to setup for the next phase
				waitForLast1.Wait()
			}

			{
				// sort a part of the nextLevel in equal chunks
				blockSize := (len(nextLevel.Nodes) + procs - 1) / procs

				low := blockSize * gid
				high := low + blockSize
				if high > len(nextLevel.Nodes) {
					high = len(nextLevel.Nodes)
				}

				if low < len(nextLevel.Nodes) {
					zuint32.SortBYOB(nextLevel.Nodes[low:high], currentLevel.Nodes[low:high])
					// update the vertLevels
					//    sentinels are sorted to the end of the array,
					//    so we can break when we find the first one
					for _, v := range nextLevel.Nodes[low:high] {
						if v == SentinelNode {
							break
						}
						level[v] = levelNumber
					}
				}
			}

			// similarly to before, the last one finishing, does the setup for next phase
			if atomic.AddInt32(&doneProcessingCounter, -1) == 0 {
				{
					levelNumber++
					currentLevel, nextLevel = nextLevel, currentLevel

					nextLevel.Nodes = nextLevel.Nodes[:cap(nextLevel.Nodes)]
					nextLevel.Head = 0

					// if we are done, set the allDone flag
					if len(currentLevel.Nodes) == 0 {
						atomic.StoreUint32(&allDone, 1)
					}
				}

				// reset counters
				atomic.StoreInt32(&doneProcessingCounter, int32(procs))
				waitForLast1.Add(1)
				// release the hounds
				waitForLast2.Done()
			} else {
				// wait for the last one to finish
				waitForLast2.Wait()
			}
		}
	}

	for len(currentLevel.Nodes) > 0 {
		async.Run(procs, func(i int) {
			runtime.LockOSThread()
			process(g, currentLevel, nextLevel, visited)
		})

		async.BlockIter(int(nextLevel.Head), procs, func(low, high int) {
			runtime.LockOSThread()
			zuint32.SortBYOB(nextLevel.Nodes[low:high], currentLevel.Nodes[low:high])
			for _, neighbor := range nextLevel.Nodes[low:high] {
				if neighbor == SentinelNode {
					break
				}
				level[neighbor] = levelNumber
			}
		})

		levelNumber++
		currentLevel, nextLevel = nextLevel, currentLevel

		currentLevel.Nodes = currentLevel.Nodes[:currentLevel.Head]
		currentLevel.Head = 0

		nextLevel.Nodes = nextLevel.Nodes[:cap(nextLevel.Nodes)]
		nextLevel.Head = 0
	}

	for gid := 1; gid < procs; gid++ {
		go worker(gid)
	}
	worker(0)
}
