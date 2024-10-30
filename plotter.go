package main

import (
	"os"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

// randomPoints returns some random x, y points.
func toXY(data []float64) plotter.XYs {
	pts := make(plotter.XYs, len(data))
	for index, dot := range data {
		pts[index].X = float64(index)
		pts[index].Y = dot
	}
	return pts
}

func GenerateDotPlot(dotData []float64) (string, error) {
	p := plot.New()

	p.Title.Text = "bsky dot"
	p.X.Label.Text = "time"
	p.Y.Label.Text = "bsky dot"

	err := plotutil.AddLinePoints(p, "Dot", toXY(dotData))
	if err != nil {
		return "", err
	}

	// Save the plot to a PNG file.
	fd, err := os.CreateTemp("", "*test.png")
	if err := p.Save(4*vg.Inch, 4*vg.Inch, fd.Name()); err != nil {
		return "", err
	}
	return fd.Name(), nil
}
