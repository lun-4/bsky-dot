package main

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorgonia.org/tensor"
)

func _Test_CosineSimilarity(t *testing.T) {

	vecA := tensor.New(tensor.WithShape(3), tensor.Of(tensor.Float64), tensor.WithBacking([]float64{1, 2, 3}))
	vecB := tensor.New(tensor.WithShape(3), tensor.Of(tensor.Float64), tensor.WithBacking([]float64{4, 5, 6}))
	simAB, err := CosineSimilarity2(vecA, vecB)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, simAB, 0.97)

	v1meta := EMBEDDING_META["v1"]

	vec1 := tensor.New(v1meta.Shape(), tensor.WithBacking(zeroEmbedding(v1meta.size, 0.1)))
	vec2 := tensor.New(v1meta.Shape(), tensor.WithBacking(zeroEmbedding(v1meta.size, 0.9)))
	sim1, err := CosineSimilarity2(vec1, vec1)
	sim11, err := CosineSimilarity2(vec1, vec1)
	assert.NoError(t, err)
	assert.Equal(t, sim1, sim11)
	sim2, err := CosineSimilarity2(vec1, vec2)
	sim22, err := CosineSimilarity2(vec2, vec1)
	assert.NoError(t, err)
	assert.Equal(t, sim2, sim22)
	fmt.Println(sim1, sim2)
	assert.NotEqual(t, sim1, sim2)

}

func _TestMapProportions(t *testing.T) {
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
