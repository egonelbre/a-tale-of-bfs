package graph

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

func LoadText(filename string) (*Graph, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseText(f)
}

func ParseText(r io.Reader) (*Graph, error) {
	graph := &Graph{}
	graph.List = make([]Node, 0, 1<<20)
	graph.Span = make([]uint64, 0, 1<<20)

	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "-----" {
			break
		}

		value, err := strconv.ParseUint(line, 10, 32)
		if err != nil {
			return nil, err
		}

		graph.Span = append(graph.Span, uint64(value-1))
	}

	for scanner.Scan() {
		line := scanner.Text()

		value, err := strconv.ParseUint(line, 10, 32)
		if err != nil {
			return nil, err
		}

		graph.List = append(graph.List, Node(value-1))
	}

	return graph, nil
}
