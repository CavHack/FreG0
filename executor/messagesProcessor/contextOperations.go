package messagesProcessor

import (
	"sync"

	"github.com/CavHack/FreGO"
	"github.com/CavHack/FreGO/aggregator"
	"github.com/CavHack/FreGO/algorithm"
	"github.com/pkg/errors"
)

type contextOperations struct {
	algorithm                algorithm.Algorithm
	errorChan                chan error
	addedVertices            map[string]interface{}
	addedVerticesMutex       sync.Mutex
	removedVertices          map[string]bool
	removedVerticesMutex     sync.Mutex
	changedVertexValues      map[string]interface{}
	changedVertexValuesMutex sync.Mutex
	vertexMessages           map[string]interface{}
	vertexMessagesMutex      sync.Mutex
	haltedVertices           map[string]bool
	haltedVerticesMutex      sync.Mutex
	addedEdges               map[edge]interface{}
	addedEdgesMutex          sync.Mutex
	removedEdges             map[edge]bool
	removedEdgesMutex        sync.Mutex
	changedEdgeValues        map[edge]interface{}
	changedEdgeValuesMutex   sync.Mutex
	aggregators              *aggregator.AggregatorSet
	aggregatorsMutex         sync.Mutex
}

type edge struct {
	from string
	to   string
}

func newContextOperations(algorithm algorithm.Algorithm, errorChan chan error) *contextOperations {
	result := &contextOperations{algorithm: algorithm, errorChan: errorChan}
	result.addedVertices = make(map[string]interface{})
	result.removedVertices = make(map[string]bool)
	result.changedVertexValues = make(map[string]interface{})
	result.vertexMessages = make(map[string]interface{})
	result.haltedVertices = make(map[string]bool)
	result.addedEdges = make(map[edge]interface{})
	result.removedEdges = make(map[edge]bool)
	result.changedEdgeValues = make(map[edge]interface{})
	result.aggregators = aggregator.NewSet()
	return result
}

func (op *contextOperations) AddVertex(id string, value interface{}) {
	op.addedVerticesMutex.Lock()
	defer op.addedVerticesMutex.Unlock()

	if value1, contains := op.addedVertices[id]; contains {
		var err error
		if value, err = op.algorithm.Handlers().OnDuplicateVertex(id, value1, value); err != nil {
			op.errorChan <- err
			return
		}
	}

	op.addedVertices[id] = value
}

func (op *contextOperations) RemoveVertex(id string) {
	op.removedVerticesMutex.Lock()
	defer op.removedVerticesMutex.Unlock()
	op.removedVertices[id] = true
}

func (op *contextOperations) SetVertexValue(id string, value interface{}) {
	op.changedVertexValuesMutex.Lock()
	defer op.changedVertexValuesMutex.Unlock()

	if value1, contains := op.changedVertexValues[id]; contains {
		var err error
		if value, err = op.algorithm.Handlers().OnDuplicateVertex(id, value1, value); err != nil {
			op.errorChan <- err
			return
		}
	}

	op.changedVertexValues[id] = value
}

func (op *contextOperations) SendVertexMessage(to string, message interface{}) {
	op.vertexMessagesMutex.Lock()
	defer op.vertexMessagesMutex.Unlock()

	if message1, contains := op.vertexMessages[to]; contains {
		message = op.algorithm.VertexMessageCombiner()(message1, message)
	}

	op.vertexMessages[to] = message
}

func (op *contextOperations) VoteToHalt(id string) {
	op.haltedVerticesMutex.Lock()
	defer op.haltedVerticesMutex.Unlock()
	op.haltedVertices[id] = true
}

func (op *contextOperations) AddEdge(from string, to string, value interface{}) {
	op.addedEdgesMutex.Lock()
	defer op.addedEdgesMutex.Unlock()

	edge := edge{from, to}
	if value1, contains := op.addedEdges[edge]; contains {
		var err error
		if value, err = op.algorithm.Handlers().OnDuplicateEdge(from, to, value1, value); err != nil {
			op.errorChan <- err
			return
		}
	}

	op.addedEdges[edge] = value
}

func (op *contextOperations) RemoveEdge(from string, to string) {
	op.removedEdgesMutex.Lock()
	defer op.removedEdgesMutex.Unlock()
	op.removedEdges[edge{from, to}] = true
}

func (op *contextOperations) SetEdgeValue(from string, to string, value interface{}) {
	op.changedEdgeValuesMutex.Lock()
	defer op.changedEdgeValuesMutex.Unlock()

	edge := edge{from, to}
	if value1, contains := op.changedEdgeValues[edge]; contains {
		var err error
		if value, err = op.algorithm.Handlers().OnDuplicateEdge(from, to, value1, value); err != nil {
			op.errorChan <- err
			return
		}
	}

	op.changedEdgeValues[edge] = value
}

func (op *contextOperations) SetAggregator(id string, aggType string, value interface{}) {
	op.aggregatorsMutex.Lock()
	defer op.aggregatorsMutex.Unlock()

	if !op.aggregators.Contains(id) {
		if err := op.aggregators.Add(id, aggType); err != nil {
			op.errorChan <- err
			return
		}
	}

	if err := op.aggregators.SetValue(id, value); err != nil {
		op.errorChan <- err
	}
}

func (op *contextOperations) RemoveAggregator(id string) {
	op.aggregatorsMutex.Lock()
	defer op.aggregatorsMutex.Unlock()

	if err := op.aggregators.Remove(id); err != nil {
		op.errorChan <- err
	}
}

func (op *contextOperations) GetEntities(jobId string, superstep int) (*ProcessResultEntities, error) {
	m := make([]*freGO.VertexMessage, 0, len(op.vertexMessages))
	v := make([]*freGO.VertexOperation, 0, op.getVertexOperationsCount())
	h := make([]*freGO.VertexHalted, 0, len(op.haltedVertices))
	e := make([]*freGO.EdgeOperation, 0, op.getEdgeOperationsCount())

	for id, value := range op.addedVertices {
		bytes, err := op.algorithm.VertexValueEncoder().Marshal(value)
		if err != nil {
			return nil, errors.Wrapf(err, "marshal failed - addedVertices: %s", id)
		}

		v = append(v, &freGO.VertexOperation{
			ID:        id,
			JobID:     jobId,
			Superstep: superstep,
			Type:      pregel.VertexAdded,
			Value:     bytes,
		})
	}

	for id := range op.removedVertices {
		v = append(v, &freGO.VertexOperation{
			ID:        id,
			JobID:     jobId,
			Superstep: superstep,
			Type:      pregel.VertexRemoved,
			Value:     nil,
		})
	}

	for id, value := range op.changedVertexValues {
		bytes, err := op.algorithm.VertexMutableValueEncoder().Marshal(value)
		if err != nil {
			return nil, errors.Wrapf(err, "marshal failed - changedVertexValues: %s", id)
		}

		v = append(v, &pregel.VertexOperation{
			ID:        id,
			JobID:     jobId,
			Superstep: superstep,
			Type:      pregel.VertexValueChanged,
			Value:     bytes,
		})
	}

	for edge, value := range op.addedEdges {
		bytes, err := op.algorithm.EdgeValueEncoder().Marshal(value)
		if err != nil {
			return nil, errors.Wrapf(err, "marshal failed - addedEdges: %+v", edge)
		}

		e = append(e, &pregel.EdgeOperation{
			From:      edge.from,
			To:        edge.to,
			JobID:     jobId,
			Superstep: superstep,
			Type:      pregel.EdgeAdded,
			Value:     bytes,
		})
	}

	for edge := range op.removedEdges {
		e = append(e, &pregel.EdgeOperation{
			From:      edge.from,
			To:        edge.to,
			JobID:     jobId,
			Superstep: superstep,
			Type:      pregel.EdgeRemoved,
			Value:     nil,
		})
	}

	for edge, value := range op.changedEdgeValues {
		bytes, err := op.algorithm.EdgeMutableValueEncoder().Marshal(value)
		if err != nil {
			return nil, errors.Wrapf(err, "marshal failed - changedEdgeValues: %+v", edge)
		}

		e = append(e, &pregel.EdgeOperation{
			From:      edge.from,
			To:        edge.to,
			JobID:     jobId,
			Superstep: superstep,
			Type:      pregel.EdgeValueChanged,
			Value:     bytes,
		})
	}

	for id, message := range op.vertexMessages {
		bytes, err := op.algorithm.VertexMessageEncoder().Marshal(message)
		if err != nil {
			return nil, errors.Wrapf(err, "marshal failed - vertexMessages: %s", id)
		}

		m = append(m, &pregel.VertexMessage{
			To:        id,
			JobID:     jobId,
			Superstep: superstep,
			Value:     bytes,
		})
	}

	for id := range op.haltedVertices {
		h = append(h, &pregel.VertexHalted{
			ID:        id,
			JobID:     jobId,
			Superstep: superstep,
		})
	}

	return &ProcessResultEntities{m, v, h, e}, nil
}

func (op *contextOperations) getVertexOperationsCount() int {
	result := len(op.addedVertices)
	result += len(op.removedVertices)
	result += len(op.changedVertexValues)
	result += len(op.haltedVertices)
	return result
}

func (op *contextOperations) getEdgeOperationsCount() int {
	result := len(op.addedEdges)
	result += len(op.removedEdges)
	result += len(op.changedEdgeValues)
	return result
}
