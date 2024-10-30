package main

import (
	"fmt"
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
