package main

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorgonia.org/tensor"
)

func Test_CosineSimilarity(t *testing.T) {
	vec1 := tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(zeroEmbedding(0)))
	vec2 := tensor.New(tensor.WithShape(1, 768), tensor.WithBacking(zeroEmbedding(1)))
	sim1, err := CosineSimilarity(vec1, vec1)
	sim11, err := CosineSimilarity(vec1, vec1)
	assert.NoError(t, err)
	assert.Equal(t, sim1, sim11)
	sim2, err := CosineSimilarity(vec1, vec2)
	sim22, err := CosineSimilarity(vec2, vec1)
	assert.NoError(t, err)
	fmt.Println(sim1, sim2)
	assert.Equal(t, sim2, sim22)
	assert.NotEqual(t, sim1, sim2)

}

func TestMapProportions(t *testing.T) {
	tests := []struct {
		name        string
		proportions []float64
		want        float64
		tolerance   float64
	}{
		{
			name:        "full A should be near -1",
			proportions: []float64{1, 0, 0},
			want:        -0.905,
			tolerance:   0.001,
		},
		{
			name:        "full C should be near 1",
			proportions: []float64{0, 0, 1},
			want:        0.905,
			tolerance:   0.001,
		},
		{
			name:        "equal distribution should be near 0",
			proportions: []float64{0.33, 0.33, 0.33},
			want:        0,
			tolerance:   0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MapProportions(tt.proportions)
			if math.Abs(got-tt.want) > tt.tolerance {
				t.Errorf("MapProportions() = %v, want %v ± %v",
					got, tt.want, tt.tolerance)
			}
		})
	}
}
