package main

import (
	"os"

	"github.com/samber/lo"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

// randomPoints returns some random x, y points.
func toXY(data []Dot, dotVersion string) plotter.XYs {
	pts := make(plotter.XYs, len(data))
	for index, dot := range data {
		pts[index].X = float64(dot.UnixTimestamp)
		var dotValue DotImpl
		switch dotVersion {
		case "v1":
			dotValue = lo.ToPtr(NewDotV1(dot.Value))
		case "v2":
			dotValue = lo.ToPtr(NewDotV2(dot.Value))
		default:
			panic("unsupported version")

		}
		pts[index].Y = dotValue.Value()
	}
	return pts
}

func GenerateDotPlot(dotData []Dot, dotVersion string) (string, error) {
	p := plot.New()

	p.Title.Text = "bsky dot"
	p.X.Label.Text = "time"
	p.Y.Label.Text = "bsky dot"

	err := plotutil.AddLinePoints(p, "Dot", toXY(dotData, dotVersion))
	if err != nil {
		return "", err
	}

	// Save the plot to a PNG file.
	fd, err := os.CreateTemp("", "*test.png")
	if err := p.Save(10*vg.Inch, 5*vg.Inch, fd.Name()); err != nil {
		return "", err
	}
	return fd.Name(), nil
}
