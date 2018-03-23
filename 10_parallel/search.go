package search

import (
	"runtime"
	"sync"

	"github.com/egonelbre/a-tale-of-bfs/graph"
	"github.com/shawnsmithdev/zermelo/zuint32"
)

func process(ch chan<- []graph.Node, g *graph.Graph, block []graph.Node, visited *NodeSet) {
	for _, v := range block {
		neighbors := make([]graph.Node, 0)
		for _, neighbor := range g.Neighbors(v) {
			if !visited.Contains(neighbor) {
				visited.Add(neighbor)
				neighbors = append(neighbors, neighbor)
			}
		}
		ch <- neighbors
	}
}

func BreadthFirst(g *graph.Graph, source graph.Node, level []int) {
	if len(level) != g.Order() {
		panic("invalid level length")
	}

	np := runtime.GOMAXPROCS(-1) / 4

	visited := NewNodeSet(g.Order())

	currentLevel := make([]graph.Node, 0, g.Order())
	nextLevel := make([]graph.Node, 0, g.Order())

	level[source] = 1
	visited.Add(source)
	currentLevel = append(currentLevel, source)

	levelNumber := 2

	for len(currentLevel) > 0 {
		var wg sync.WaitGroup

		chunkSize := (len(currentLevel) + np - 1) / np
		var workblocks [][]graph.Node
		for i := 0; i < len(currentLevel); i += chunkSize {
			end := i + chunkSize
			if end > len(currentLevel) {
				end = len(currentLevel)
			}
			workblocks = append(workblocks, currentLevel[i:end])
		}

		ch := make(chan []uint32, len(workblocks))
		wg.Add(len(workblocks))
		for _, block := range workblocks {
			go func(block []graph.Node) {
				process(ch, g, block, &visited)
				wg.Done()
			}(block)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()

		for ns := range ch {
			nextLevel = append(nextLevel, ns...)
		}

		zuint32.SortBYOB(nextLevel, currentLevel[:cap(currentLevel)])

		for _, neighbor := range nextLevel {
			level[neighbor] = levelNumber
		}

		levelNumber++
		currentLevel = currentLevel[:0:cap(currentLevel)]
		currentLevel, nextLevel = nextLevel, currentLevel
	}
}
