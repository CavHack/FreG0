package algorithm

import (
	"github.com/CavHack/FreGO/aggregator"
	"github.com/CavHack/FreGO/encoding"
)

type Algorithm interface {
	Compute(context *VertexContext, message interface{}) error
	GetResult(aggregators *aggregator.ImmutableAggregatorSet) interface{}
	GetResultDisplayValue(result interface{}) string

	VertexMessageCombiner() VertexMessageCombiner

	VertexMessageEncoder() encoding.Encoder
	VertexValueEncoder() encoding.Encoder
	EdgeValueEncoder() encoding.Encoder
	VertexMutableValueEncoder() encoding.Encoder
	EdgeMutableValueEncoder() encoding.Encoder
	ResultEncoder() encoding.Encoder

	Handlers() *Handlers
}

type VertexMessageCombiner func(first interface{}, second interface{}) interface{}

type ContextOperations interface {
	AddVertex(id string, value interface{})
	RemoveVertex(id string)
	SetVertexValue(id string, value interface{})
	SendVertexMessage(to string, message interface{})

	VoteToHalt(id string)

	AddEdge(from string, to string, value interface{})
	RemoveEdge(from string, to string)
	SetEdgeValue(from string, to string, value interface{})

	SetAggregator(id string, aggType string, value interface{})
	RemoveAggregator(id string)
}
