package main

import (
	"fmt"
	"image/color"
	"io"
	"io/ioutil"
	"os"

	"github.com/loov/csvcolumn"
	"github.com/loov/diagram"
)

func main() {
	xeon, xeonerr := ParseFile("../results/Linux-Xeon-E5-2670v3.txt")
	wii7, wii7err := ParseFile("../results/Win-i7-2820QM.txt")
	if xeonerr != nil || wii7err != nil {
		fmt.Fprintln(os.Stderr, xeonerr)
		fmt.Fprintln(os.Stderr, wii7err)
		os.Exit(1)
	}
	xeon = xeon.Dataset("friendster")
	wii7 = wii7.Dataset("sg-5m-100m")

	baseline := Line{"baseline", wii7.A("baseline"), xeon.A("baseline")}
	reuse := Line{"reuse level", wii7.A("reuse level"), xeon.A("reuse level")}
	sort := Line{"sort", wii7.A("sort"), xeon.A("sort")}
	sort_inline := Line{"inline sort", wii7.A("inline sort"), xeon.A("inline sort")}
	sort_radix := Line{"radix sort", wii7.A("radix sort"), xeon.A("radix sort")}
	lift_level := Line{"lift level", wii7.A("lift level"), xeon.A("lift level")}
	ordering := Line{"ordering", wii7.A("ordering"), xeon.A("ordering")}
	fused := Line{"fused", wii7.A("fused"), xeon.A("fused")}
	fusedif := Line{"fused if", wii7.A("fused if"), xeon.A("fused if")}
	cuckoo := Line{"cuckoo", wii7.A("cuckoo"), xeon.A("cuckoo")}
	unroll_4 := Line{"unroll 4", wii7.A("unroll 4"), xeon.A("unroll 4")}
	unroll_8 := Line{"unroll 8", wii7.A("unroll 8"), xeon.A("unroll 8")}
	unroll_8_4 := Line{"unroll 8 4", wii7.A("unroll 8 4"), xeon.A("unroll 8 4")}
	parallel := Line{"parallel", wii7.A("parallel"), xeon.A("parallel")}

	frontier_4x := Line{"frontier", wii7.A("frontier 4x"), xeon.A("frontier 4x")}
	almost_4x := Line{"almost", wii7.A("almost 4x"), xeon.A("almost 4x")}
	marking_4x := Line{"marking", wii7.A("marking 4x"), xeon.A("marking 4x")}
	early2_4x := Line{"early2", wii7.A("early2 4x"), xeon.A("early2 4x")}
	early3_4x := Line{"early3", wii7.A("early3 4x"), xeon.A("early3 4x")}
	early4_4x := Line{"early4", wii7.A("early4 4x"), xeon.A("early4 4x")}
	earlyR_4x := Line{"earlyR", wii7.A("earlyR 4x"), xeon.A("earlyR 4x")}
	worker_4x := Line{"worker", wii7.A("worker 4x"), xeon.A("worker 4x")}
	busy_4x := Line{"busy", wii7.A("busy 4x"), xeon.A("busy 4x")}

	frontier_48x := Line{"frontier", wii7.A("frontier 8x"), xeon.A("frontier 48x")}
	almost_48x := Line{"almost", wii7.A("almost 8x"), xeon.A("almost 48x")}
	marking_48x := Line{"marking", wii7.A("marking 8x"), xeon.A("marking 48x")}
	early2_48x := Line{"early2", wii7.A("early2 8x"), xeon.A("early2 48x")}
	early3_48x := Line{"early3", wii7.A("early3 8x"), xeon.A("early3 48x")}
	early4_48x := Line{"early4", wii7.A("early4 8x"), xeon.A("early4 48x")}
	earlyR_48x := Line{"earlyR", wii7.A("earlyR 8x"), xeon.A("earlyR 48x")}
	worker_48x := Line{"worker", wii7.A("worker 8x"), xeon.A("worker 48x")}
	busy_48x := Line{"busy", wii7.A("busy 8x"), xeon.A("busy 48x")}

	Plot("00-baseline.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
	)

	Plot("01-reuse-levels.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		reuse,
	)

	Plot("02-sort.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		reuse,
		sort,
	)

	Plot("03-sort-inline.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		reuse,
		sort,
		sort_inline,
	)

	Plot("04-sort-radix.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		reuse,
		sort,
		sort_inline,
		sort_radix,
	)

	Plot("05-lift-level.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		sort,
		sort_inline,
		sort_radix,
		lift_level,
	)

	Plot("06.0-ordering.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		sort_radix,
		lift_level,
		ordering,
	)

	Plot("06.1-fusing.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		sort_radix,
		lift_level,
		ordering,
		fused,
		fusedif,
	)

	Plot("07-cuckoo.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		ordering,
		cuckoo,
	)

	Plot("08-unroll.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		ordering,
		unroll_4,
		unroll_8,
		unroll_8_4,
	)

	Plot("09-summary.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		reuse,
		sort,
		sort_inline,
		sort_radix,
		lift_level,
		ordering,
		fused,
		fusedif,
		cuckoo,
		unroll_4,
		unroll_8,
		unroll_8_4,
	)

	Plot("10-parallel.svg", "i7-2820QM | 5M nodes", "65M nodes | E5-2670v3",
		baseline,
		unroll_8,
		parallel,
	)

	Plot("11-frontier-48x.svg", "8x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 48x",
		baseline,
		unroll_8,
		parallel,
		frontier_48x,
	)

	Plot("12-almost-48x.svg", "8x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 48x",
		baseline,
		unroll_8,
		frontier_48x,
		almost_48x,
		marking_48x,
	)

	Plot("13-early-48x.svg", "8x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 48x",
		baseline,
		unroll_8,
		frontier_48x,
		marking_48x,
		early2_48x,
		early3_48x,
		early4_48x,
		earlyR_48x,
	)

	Plot("14-workers-48x.svg", "8x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 48x",
		baseline,
		unroll_8,
		frontier_48x,
		marking_48x,
		early4_48x,
		worker_48x,
	)

	Plot("15-busy-48x.svg", "8x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 48x",
		baseline,
		unroll_8,
		frontier_48x,
		marking_48x,
		early4_48x,
		worker_48x,
		busy_48x,
	)

	Plot("19-final_48x.svg", "8x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 48x",
		baseline,
		unroll_8,
		parallel,
		frontier_48x,
		almost_48x,
		marking_48x,
		early2_48x,
		early3_48x,
		early4_48x,
		earlyR_48x,
		worker_48x,
		busy_48x,
	)

	Plot("19-final_4x.svg", "4x i7-2820QM | 5M nodes", "65M nodes | E5-2670v3 4x",
		baseline,
		unroll_8,
		parallel,
		frontier_4x,
		almost_4x,
		marking_4x,
		early2_4x,
		early3_4x,
		early4_4x,
		earlyR_4x,
		worker_4x,
		busy_4x,
	)
}

func Plot(filename string, lefttitle, righttitle string, lines ...Line) {
	const (
		head       = 20
		sidewidth  = 350
		height     = 16
		textheight = 16
		textwidth  = textheight * 12
		pad        = 2

		sleft  = 10000
		sright = 50000
	)

	const margin = 30
	canvaswidth := float64(sidewidth*2 + textwidth + margin*2)
	canvasheight := float64(head + (height+2*pad)*len(lines) + margin*2)

	canvas := diagram.NewSVG(canvaswidth, canvasheight)
	r := canvas.Bounds().Shrink(diagram.Point{margin, margin})

	inner := canvas.Context(r)
	base := inner.Layer(0)
	grid := inner.Layer(1)
	text := inner.Layer(2)

	var xl float64 = sidewidth
	var xr float64 = sidewidth + textwidth

	var left diagram.Rect
	left.Max.X = xl
	left.Max.Y = height

	var center diagram.Rect
	center.Min.X = xl
	center.Max.X = xr
	center.Max.Y = height

	var right diagram.Rect
	right.Min.X = xr
	right.Max.Y = height

	black := color.Gray16{0}
	//white := color.Gray16{0xffff}

	pleft := func(v float64) float64 {
		return xl - v*sidewidth/sleft
	}
	pright := func(v float64) float64 {
		return xr + v*sidewidth/sright
	}

	text.Text(lefttitle, diagram.Point{xl, -pad},
		&diagram.Style{
			Fill:   black,
			Size:   14,
			Origin: diagram.Point{0, -1},
		})
	text.Text(righttitle, diagram.Point{xr, -pad},
		&diagram.Style{
			Fill:   black,
			Size:   14,
			Origin: diagram.Point{0, -1},
		})

	for k := 0; k <= 10; k += 1 {
		x := pleft(float64(k) * 1000)
		grid.Poly(diagram.Ps(
			x, head,
			x, grid.Bounds().Size().Y,
		), &diagram.Style{
			Stroke: color.Gray16{0x4444},
			Size:   1,
		})

		grid.Text(fmt.Sprintf("%ds", k),
			diagram.Point{x, head},
			&diagram.Style{
				Fill:   black,
				Size:   14,
				Origin: diagram.Point{0, 1},
			})
	}
	for k := 0; k <= 50; k += 5 {
		x := pright(float64(k) * 1000)
		grid.Poly(diagram.Ps(
			x, head,
			x, grid.Bounds().Size().Y,
		), &diagram.Style{
			Stroke: color.Gray16{0x4444},
			Size:   1,
		})

		grid.Text(fmt.Sprintf("%ds", k),
			diagram.Point{x, head},
			&diagram.Style{
				Fill:   black,
				Size:   14,
				Origin: diagram.Point{0, 1},
			})
	}

	left = left.Offset(diagram.Point{Y: head})
	center = center.Offset(diagram.Point{Y: head})
	right = right.Offset(diagram.Point{Y: head})

	for _, line := range lines {
		left = left.Offset(diagram.Point{Y: pad})
		center = center.Offset(diagram.Point{Y: pad})
		right = right.Offset(diagram.Point{Y: pad})

		left.Min.X = pleft(line.Left.Median)
		right.Max.X = pright(line.Right.Median)

		y := center.Max.Y - pad

		base.Rect(left, &diagram.Style{Fill: black})
		text.Text(
			fmt.Sprintf("%.2fs", line.Left.Median/1000),
			diagram.Point{
				X: left.Max.X + pad,
				Y: y,
			}, &diagram.Style{
				Fill:   black,
				Size:   textheight * 0.9,
				Origin: diagram.Point{-1, 1},
			})

		base.Rect(right, &diagram.Style{Fill: black})
		text.Text(
			fmt.Sprintf("%.2fs", line.Right.Median/1000),
			diagram.Point{
				X: right.Min.X - pad,
				Y: y,
			}, &diagram.Style{
				Fill:   black,
				Size:   textheight * 0.9,
				Origin: diagram.Point{1, 1},
			})

		text.Text(line.Name, diagram.Point{
			X: (center.Min.X + center.Max.X) / 2,
			Y: y,
		}, &diagram.Style{
			Fill:   black,
			Size:   textheight,
			Font:   "bold",
			Origin: diagram.Point{0, 1},
		})

		left = left.Offset(diagram.Point{Y: height + pad})
		center = center.Offset(diagram.Point{Y: height + pad})
		right = right.Offset(diagram.Point{Y: height + pad})
	}

	ioutil.WriteFile(filename, canvas.Bytes(), 0644)
}

type Lines []Line

type Line struct {
	Name  string
	Left  Measurement
	Right Measurement
}

type Entry struct {
	Dataset  string
	Approach string
}

type Measurements []Measurement

func (xs Measurements) Dataset(dataset string) Measurements {
	var rs Measurements
	for _, x := range xs {
		if x.Dataset == dataset {
			rs = append(rs, x)
		}
	}
	return rs
}

func (xs Measurements) E(dataset, approach string) Measurement {
	return xs.Entry(Entry{dataset, approach})
}

func (xs Measurements) A(approach string) Measurement {
	for _, x := range xs {
		if x.Approach == approach {
			return x
		}
	}
	return Measurement{}
}

func (xs Measurements) Entry(e Entry) Measurement {
	for _, x := range xs {
		if x.Entry == e {
			return x
		}
	}
	return Measurement{}
}

type Measurement struct {
	Entry
	Median  float64
	Average float64
	Stdev   float64
	Min     float64
	Max     float64
}

func ParseFile(name string) (Measurements, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ParseMeasurements(f)
}

func ParseMeasurements(in io.Reader) (Measurements, error) {
	data := csvcolumn.NewReader(in)
	data.LazyQuotes = true
	data.Comma = '\t'

	dataset, approach := data.String("dataset"), data.String("approach")
	med, avg, stdev := data.Float64("med"), data.Float64("avg"), data.Float64("stdev")
	min, max := data.Float64("min"), data.Float64("max")

	var xs Measurements
	for data.Next() && data.Err() == nil {
		var x Measurement
		x.Dataset = *dataset
		x.Approach = *approach
		x.Median = *med
		x.Average = *avg
		x.Stdev = *stdev
		x.Min = *min
		x.Max = *max
		xs = append(xs, x)
	}
	if err := data.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse: %w", err)
	}
	return xs, nil
}
