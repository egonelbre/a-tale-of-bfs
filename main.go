package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

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

	s10_parallel "github.com/egonelbre/a-tale-of-bfs/10_parallel"
	s10_parchan "github.com/egonelbre/a-tale-of-bfs/10_parchan"
	s11_frontier "github.com/egonelbre/a-tale-of-bfs/11_frontier"
	s12_almost "github.com/egonelbre/a-tale-of-bfs/12_almost"
	s13_marking "github.com/egonelbre/a-tale-of-bfs/13_marking"

	s14_early_2 "github.com/egonelbre/a-tale-of-bfs/14_early_2"
	s14_early_3 "github.com/egonelbre/a-tale-of-bfs/14_early_3"
	s14_early_4 "github.com/egonelbre/a-tale-of-bfs/14_early_4"
	s14_early_r "github.com/egonelbre/a-tale-of-bfs/14_early_r"

	s15_worker "github.com/egonelbre/a-tale-of-bfs/15_worker"
	s16_busy "github.com/egonelbre/a-tale-of-bfs/16_busy"
)

var (
	cold = flag.Bool("cold", false, "also include cold run")
	run  = flag.String("run", "", "filter approaches")
	N    = flag.Int("N", 10, "benchmark iterations")
)

type IterateFn func(g *graph.Graph, source graph.Node, levels []int)
type IterateFnParallel func(g *graph.Graph, source graph.Node, levels []int, procs int)

func IterateParallel(procs int, iterate IterateFnParallel) IterateFn {
	return func(g *graph.Graph, source graph.Node, levels []int) {
		iterate(g, source, levels, procs)
	}
}

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

func Test(g *graph.Graph, name string, source graph.Node, iterate IterateFn, expected []int) {
	levels := make([]int, g.Order())

	done := make(chan struct{})
	go func() {
		iterate(g, source, levels)
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		fmt.Fprintln(os.Stderr, "Locked ", name)
		return
	}

	levelCounts := []int{}
	for _, level := range levels {
		if level >= len(levelCounts) {
			levelCounts = append(levelCounts, make([]int, level-len(levelCounts)+1)...)
		}
		levelCounts[level]++
	}

	if !reflect.DeepEqual(levelCounts, expected) {
		fmt.Fprintln(os.Stderr, "Invalid ", name, "got", levelCounts, "exp", expected)
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

	return fmt.Sprintf("%v\t%v\t%v\t%v\t%v", ms(q), ms(mean), ms(variance), ms(min), ms(max))
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

	max := runtime.GOMAXPROCS(-1)
	maxs := fmt.Sprintf("%dx", max)

	iterators := []struct {
		Name    string
		Iterate IterateFn
		Skip    bool
	}{
		{"baseline", s00_baseline.BreadthFirst, false},
		{"reuse level", s01_reuse_level.BreadthFirst, false},
		{"sort", s02_sort.BreadthFirst, false},
		{"inline sort", s03_inline_sort.BreadthFirst, false},
		{"radix sort", s04_radix_sort.BreadthFirst, false},
		{"lift level", s05_lift_level.BreadthFirst, false},

		{"ordering", s06_ordering.BreadthFirst, false},
		{"fused", s07_fused.BreadthFirst, false},
		{"fused if", s07_fused_if.BreadthFirst, false},
		{"cuckoo", s08_cuckoo.BreadthFirst, true},

		{"unroll 4", s09_unroll_4.BreadthFirst, false},
		{"unroll 8", s09_unroll_8.BreadthFirst, false},
		{"unroll 8 4", s09_unroll_8_4.BreadthFirst, false},

		{"parallel", s10_parallel.BreadthFirst, true},
		{"parchan 4x", IterateParallel(4, s10_parchan.BreadthFirst), false},
		{"parchan " + maxs, IterateParallel(max, s10_parchan.BreadthFirst), false},

		{"frontier 4x", IterateParallel(4, s11_frontier.BreadthFirst), false},
		{"frontier " + maxs, IterateParallel(max, s11_frontier.BreadthFirst), false},

		{"almost 4x", IterateParallel(4, s12_almost.BreadthFirst), false},
		{"almost " + maxs, IterateParallel(max, s12_almost.BreadthFirst), false},

		{"marking 4x", IterateParallel(4, s13_marking.BreadthFirst), false},
		{"marking " + maxs, IterateParallel(max, s13_marking.BreadthFirst), false},

		{"early2 4x", IterateParallel(4, s14_early_2.BreadthFirst), false},
		{"early2 " + maxs, IterateParallel(max, s14_early_2.BreadthFirst), false},

		{"early3 4x", IterateParallel(4, s14_early_3.BreadthFirst), false},
		{"early3 " + maxs, IterateParallel(max, s14_early_3.BreadthFirst), false},

		{"early4 4x", IterateParallel(4, s14_early_4.BreadthFirst), false},
		{"early4 " + maxs, IterateParallel(max, s14_early_4.BreadthFirst), false},

		{"earlyR 4x", IterateParallel(4, s14_early_r.BreadthFirst), false},
		{"earlyR " + maxs, IterateParallel(max, s14_early_r.BreadthFirst), false},

		{"worker 4x", IterateParallel(4, s15_worker.BreadthFirst), false},
		{"worker " + maxs, IterateParallel(max, s15_worker.BreadthFirst), false},

		{"busy 4x", IterateParallel(4, s16_busy.BreadthFirst), false},
		{"busy " + maxs, IterateParallel(max, s16_busy.BreadthFirst), false},
	}

	for _, it := range iterators {
		Test(g10k, it.Name, SOURCE, it.Iterate, []int{490000, 1, 55, 2416, 7528})
	}

	rx := regexp.MustCompile(*run)

	//w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	// defer w.Flush()

	w := os.Stdout
	fmt.Fprintf(w, "dataset\tapproach\tmed\tavg\tvar\tmin\tmax\n")
	for _, dataset := range datasets {
		fmt.Fprintln(os.Stderr, "# Dataset", dataset.Name)
		for _, it := range iterators {
			if *run != "" && !rx.MatchString(it.Name) {
				continue
			}

			fmt.Fprint(os.Stderr, "  > ", it.Name, "\t")

			if *cold {
				EmptyRun(dataset.Graph, SOURCE, it.Iterate)
			}

			n := *N
			if it.Skip {
				n = 1
			}

			timings := Benchmark(dataset.Graph, SOURCE, it.Iterate, n)
			stats := Stats(timings)
			fmt.Fprintln(os.Stderr, stats)
			fmt.Fprintf(w, "%v\t%v\t%v\n", dataset.Name, it.Name, stats)
		}
	}
	fmt.Fprint(os.Stderr, "\n")
}

func removeExt(name string) string {
	p := strings.Index(name, ".")
	if p < 0 {
		return name
	}
	return name[:p]
}
