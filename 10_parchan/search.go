package search

import (
	"runtime"

	"github.com/egonelbre/a-tale-of-bfs/graph"
	"github.com/egonelbre/async"
)

func BreadthFirst(g *graph.Graph, source graph.Node, level []int, procs int) {
	if len(level) != g.Order() {
		panic("invalid level length")
	}

	visited := NewNodeSet(g.Order())

	currentLevel := make(chan graph.Node, g.Order())
	nextLevel := make(chan graph.Node, g.Order())

	level[source] = 1
	visited.Add(source)
	currentLevel <- source

	levelNumber := 2
	for len(currentLevel) > 0 {
		async.Run(procs, func(gid int) {
			runtime.LockOSThread()
			for {
				select {
				case node := <-currentLevel:
					for _, neighbor := range g.Neighbors(node) {
						if visited.TryAdd(neighbor) {
							level[neighbor] = levelNumber
							nextLevel <- neighbor
						}
					}
				default:
					// queue is empty
					return
				}
			}
		})

		// :( we cannot sort here

		levelNumber++
		currentLevel, nextLevel = nextLevel, currentLevel
	}
}
