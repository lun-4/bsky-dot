package main

import "gorgonia.org/tensor"

//const EMBEDDING_V1_SIZE = 768
//const EMBEDDING_V1_MODEL = "nomic-embed-text-v1.5.Q8_0.gguf"

type Embedding struct {
	name  string
	model string

	// dimensions of the embedding model
	size uint
}

func (e Embedding) Shape() tensor.ConsOpt {
	return tensor.WithShape(int(e.size))
}

var EMBEDDING_META = map[string]Embedding{
	"v1": {
		name:  "v1",
		model: "nomic-embed-text-v1.5.Q8_0.gguf",
		size:  768,
	},
}
