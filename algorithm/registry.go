package algorithm

import (
	"fmt"

	"github.com/CavHack/FreGO/encoding"
)

type AlgorithmFactory interface {
	Create(params interface{}) (Algorithm, error)
	ParamsEncoder() encoding.Encoder
}

var factories = make(map[string]AlgorithmFactory)

func Register(algorithm string, factory AlgorithmFactory) {
	factories[algorithm] = factory
}

func New(algorithm string, params []byte) (Algorithm, error) {
	factory, ok := factories[algorithm]
	if !ok {
		return nil, fmt.Errorf("unknown algorithm %s", algorithm)
	}

	p, err := factory.ParamsEncoder().Unmarshal(params)
	if err != nil {
		return nil, err
	}

	return factory.Create(p)
}
