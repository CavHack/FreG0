package frego

import "time"



type JobNode struct {
     jobID              string
     jobLabel           string
     jobCreationTime    time.Time
     jobStatus          JobStatus
     jobStore           string
     jobStoreParams     []byte
     jobAlgorithm       string
     jobAlgorithmParams []byte
     jobTaskCPU         float64
     jobTaskMEM         float64
     jobTaskVertices    int
     jobTaskTimeoutSec  int
}

type JobNodeStatus int

const (
      _ JobNodeStatus = iota
      JobNodeCreated
      JobNodeRunning
      JobNodeCompleted
      JobNodeCancelled
      JobNodeFailed
)

func (j JobNode) CanCancel() bool {
     return j.jobStatus == JobNodeCreated || j.jobStatus == JobNodeRunning
}