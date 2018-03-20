package search

import (
	"github.com/egonelbre/a-tale-of-bfs/graph"
	"github.com/shawnsmithdev/zermelo/zuint32"
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
			neighbors := g.Neighbors(node)
			i := 0

			for ; i < len(neighbors)-3; i += 4 {
				n1, n2, n3, n4 := neighbors[i], neighbors[i+1], neighbors[i+2], neighbors[i+3]
				if !visited.Contains(n1) {
					visited.Add(n1)
					nextLevel = append(nextLevel, n1)
				}
				if !visited.Contains(n2) {
					visited.Add(n2)
					nextLevel = append(nextLevel, n2)
				}
				if !visited.Contains(n3) {
					visited.Add(n3)
					nextLevel = append(nextLevel, n3)
				}
				if !visited.Contains(n4) {
					visited.Add(n4)
					nextLevel = append(nextLevel, n4)
				}
			}

			for _, n := range neighbors[i:] {
				if !visited.Contains(n) {
					visited.Add(n)
					nextLevel = append(nextLevel, n)
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
