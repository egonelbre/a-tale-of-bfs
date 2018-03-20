package graph

import (
	"os"
	"unsafe"

	mmap "github.com/edsrzf/mmap-go"
)

func LoadDAT(filename string) (*Graph, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer data.Unmap()

	var listlen uint64
	var spanlen uint64

	x := 0
	copy((*[8]byte)(unsafe.Pointer(&listlen))[:], data[x:x+8])
	x += 8
	copy((*[8]byte)(unsafe.Pointer(&spanlen))[:], data[x:x+8])
	x += 8

	graph := &Graph{}
	graph.List = make([]Node, listlen)
	graph.Span = make([]uint64, spanlen)

	listdata := ((*[1 << 40]Node)(unsafe.Pointer(&data[x])))
	copy(graph.List, listdata[:])
	x += 4 * int(listlen)

	spandata := ((*[1 << 40]uint64)(unsafe.Pointer(&data[x])))
	copy(graph.Span, spandata[:])

	return graph, nil
}

func WriteDat(filename string, g *Graph) error {
	return nil
}
