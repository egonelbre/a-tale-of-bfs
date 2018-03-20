package search

import (
	"github.com/egonelbre/a-tale-of-bfs/graph"
	"github.com/shawnsmithdev/zermelo/zuint32"
)

func BreadthFirst(g *graph.Graph, source graph.Node, level []int) {
	if len(level) != g.Order() {
		panic("invalid level length")
	}

	filter := NewCuckoof(1 << 10)

	visited := NewNodeSet(g.Order())

	currentLevel := make([]graph.Node, 0, g.Order())
	nextLevel := make([]graph.Node, 0, g.Order())

	level[source] = 1
	visited.Add(source)
	currentLevel = append(currentLevel, source)

	levelNumber := 2

	for len(currentLevel) > 0 {
		for _, node := range currentLevel {
			for _, neighbor := range g.Neighbors(node) {
				if !visited.Contains(neighbor) {
					visited.Add(neighbor)
					filter.Insert(neighbor)
					nextLevel = append(nextLevel, neighbor)
				}
			}
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
