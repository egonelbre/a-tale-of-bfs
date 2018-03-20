package search

import (
	"sort"

	"github.com/egonelbre/a-tale-of-bfs/graph"
)

func BreadthFirst(g *graph.Graph, source graph.Node, level []int) {
	if len(level) != g.Order() {
		panic("invalid level length")
	}

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
					nextLevel = append(nextLevel, neighbor)
					level[neighbor] = levelNumber
					visited.Add(neighbor)
				}
			}
		}

		sort.Slice(nextLevel, func(i, k int) bool {
			return nextLevel[i] < nextLevel[k]
		})

		levelNumber++
		currentLevel = currentLevel[:0:cap(currentLevel)]
		currentLevel, nextLevel = nextLevel, currentLevel
	}
}
