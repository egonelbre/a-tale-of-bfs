package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/egonelbre/a-tale-of-bfs/graph"
	"github.com/egonelbre/exp/qpc"
	"github.com/gonum/stat"
	"gonum.org/v1/gonum/floats"

	s00_baseline "github.com/egonelbre/a-tale-of-bfs/00_baseline"
	s01_reuse_level "github.com/egonelbre/a-tale-of-bfs/01_reuse_level"
	s02_sort "github.com/egonelbre/a-tale-of-bfs/02_sort"
	s03_inline_sort "github.com/egonelbre/a-tale-of-bfs/03_inline_sort"
	s04_radix_sort "github.com/egonelbre/a-tale-of-bfs/04_radix_sort"
	s05_lift_level "github.com/egonelbre/a-tale-of-bfs/05_lift_level"

	s06_ordering "github.com/egonelbre/a-tale-of-bfs/06_ordering"
	s07_fused "github.com/egonelbre/a-tale-of-bfs/07_fused"
	s07_fused_if "github.com/egonelbre/a-tale-of-bfs/07_fused_if"
	s08_cuckoo "github.com/egonelbre/a-tale-of-bfs/08_cuckoo"

	s09_unroll_4 "github.com/egonelbre/a-tale-of-bfs/09_unroll_4"
	s09_unroll_8 "github.com/egonelbre/a-tale-of-bfs/09_unroll_8"
	s09_unroll_8_4 "github.com/egonelbre/a-tale-of-bfs/09_unroll_8_4"
)

var (
	cold = flag.Bool("cold", false, "also include cold run")
	N    = flag.Int("N", 1, "benchmark iterations")
)

type IterateFn func(g *graph.Graph, source graph.Node, levels []int)

func EmptyRun(g *graph.Graph, source graph.Node, iterate IterateFn) {
	levels := make([]int, g.Order())
	debug.SetGCPercent(0)
	runtime.GC()
	{
		iterate(g, source, levels)
	}
	debug.SetGCPercent(100)
	runtime.GC()
}

func Benchmark(g *graph.Graph, source graph.Node, iterate IterateFn, N int) []float64 {
	timings := []float64{}
	for k := 0; k < N; k++ {
		var start, stop qpc.Count
		levels := make([]int, g.Order())
		{
			debug.SetGCPercent(0)
			runtime.GC()
			{
				start = qpc.Now()
				iterate(g, source, levels)
				stop = qpc.Now()
			}
			debug.SetGCPercent(100)
			runtime.GC()
		}
		timings = append(timings, stop.Sub(start).Duration().Seconds())
	}

	return timings
}

func Test(g *graph.Graph, source graph.Node, iterate IterateFn, expected []int) {
	levels := make([]int, g.Order())
	iterate(g, source, levels)

	levelCounts := []int{}
	for _, level := range levels {
		if level >= len(levelCounts) {
			levelCounts = append(levelCounts, make([]int, level-len(levelCounts)+1)...)
		}
		levelCounts[level]++
	}

	if !reflect.DeepEqual(levelCounts, expected) {
		fmt.Fprintln(os.Stderr, "Invalid result: ", levelCounts, expected)
	}
}

func Stats(timings []float64) string {
	sort.Float64s(timings)

	min := floats.Min(timings)
	max := floats.Max(timings)
	mean, variance := stat.MeanStdDev(timings, nil)
	q := stat.Quantile(0.5, stat.Empirical, timings, nil)

	ms := func(v float64) string {
		return fmt.Sprintf("%.2f", v*1000)
	}

	return fmt.Sprintf("%v\t%v±%v\t{%v..%v}", ms(q), ms(mean), ms(variance), ms(min), ms(max))
}

func main() {
	runtime.LockOSThread()
	flag.Parse()

	type Dataset struct {
		Name  string
		Graph *graph.Graph
	}

	var datasets []Dataset
	for _, filename := range flag.Args() {
		fmt.Fprintln(os.Stderr, "# Loading dataset ", filename)
		var g *graph.Graph
		var err error
		switch filepath.Ext(filename) {
		case ".dat":
			g, err = graph.LoadDAT(filename)
		case ".txt":
			g, err = graph.LoadText(filename)
		default:
			fmt.Fprintln(os.Stderr, "unknown file format: "+filename)
			os.Exit(1)
		}

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		datasets = append(datasets, Dataset{
			Name:  removeExt(filepath.Base(filename)),
			Graph: g,
		})
	}

	const SOURCE = graph.Node(2)

	g10k, err := graph.LoadText("data/sg-10k-250k.txt")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	iterators := []struct {
		Name    string
		Iterate IterateFn
		Skip    bool
	}{
		{"baseline", s00_baseline.BreadthFirst},
		{"reuse level", s01_reuse_level.BreadthFirst},
		{"sort", s02_sort.BreadthFirst},
		{"inline sort", s03_inline_sort.BreadthFirst},
		{"radix sort", s04_radix_sort.BreadthFirst},
		{"lift level", s05_lift_level.BreadthFirst},

		{"ordering", s06_ordering.BreadthFirst},
		{"fused", s07_fused.BreadthFirst},
		{"fused if", s07_fused_if.BreadthFirst},
		{"cuckoo", s08_cuckoo.BreadthFirst, true},

		{"unroll 4", s09_unroll_4.BreadthFirst},
		{"unroll 8", s09_unroll_8.BreadthFirst},
		{"unroll 8 4", s09_unroll_8_4.BreadthFirst},
	}

	for _, it := range iterators {
		fmt.Fprintln(os.Stderr, "# Testing ", it.Name)
		Test(g10k, SOURCE, it.Iterate, []int{490000, 1, 55, 2416, 7528})
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for _, dataset := range datasets {
		fmt.Fprintln(os.Stderr, "# Dataset", dataset.Name)
		for _, it := range iterators {
			if it.Skip {
				continue
			}
			fmt.Fprint(os.Stderr, "  > ", it.Name, "\t")
			if *cold {
				EmptyRun(dataset.Graph, SOURCE, it.Iterate)
			}
			timings := Benchmark(dataset.Graph, SOURCE, it.Iterate, *N)
			stats := Stats(timings)
			fmt.Fprintln(os.Stderr, stats)
			fmt.Fprintf(w, "%v\t%v\t%v\n", dataset.Name, it.Name, stats)
		}
	}
	fmt.Fprint(os.Stderr, "\n")
	w.Flush()
}

func removeExt(name string) string {
	p := strings.Index(name, ".")
	if p < 0 {
		return name
	}
	return name[:p]
}
