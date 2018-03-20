package graph

type Node uint32

type Graph struct {
	List []Node
	Span []uint64
}

func (graph *Graph) Neighbors(n Node) []Node {
	start, end := graph.Span[n], graph.Span[n+1]
	return graph.List[start:end]
}

func (graph *Graph) Order() int {
	return len(graph.List)
}
