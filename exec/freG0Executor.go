package main

import(

    "github.com/CavHack/FreGO/enc"
    "github.com/CavHack/FreGO/exec/graph"
    "github.com/CavHack/FreGO/protos"
    "github.com/CavHack/FreGO/store"
    "github.com/gogo/protobuf/proto"
  	"github.com/golang/glog"
  	exec "github.com/mesos/mesos-go/api/v0/executor"
  	mesos "github.com/mesos/mesos-go/api/v0/mesosproto"
  	"github.com/patrickmn/go-cache"


)

const (

  cacheExpiration = 10 * time.Minute
  cacheCleanupInterval = 7 * time.Minute

  )

  type FreGOExecutor struct {

    execTaskParamsEncoder enc.Encoder
  	execTaskResultEncoder enc.Encoder
  	mutex                 sync.Mutex
  	graphPoolCache        *cache.Cache

  }

  func NewFreGOExecutor() *FreGOExecutor {
  	graphPoolCache := cache.New(cacheExpiration, cacheCleanupInterval)
  	graphPoolCache.OnEvicted(onGraphPoolEvicted)

  	return &FreGOExecutor{
  		graphPoolCache:        graphPoolCache,
  		execTaskParamsEncoder: enc.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskParams) }),
  		execTaskResultEncoder: enc.NewProtobufEncoder(func() proto.Message { return new(protos.ExecTaskResult) }),
  	}
  }

  func (executor *FreGOExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
  	glog.Infof("registered Executor on slave %s", slaveInfo.GetHostname())
  }

  func (executor *FreGOExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
  	glog.Infof("Re-registered Executor on slave %s", slaveInfo.GetHostname())
  }

  func (executor *FreGOExecutor) Disconnected(driver exec.ExecutorDriver) {
  	glog.Infof("Executor disconnected.")
  }

  func (executor *FreGOExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
  	go executor.processLaunchTask(driver, taskInfo)
  }

  func (executor *FreGOExecutor) KillTask(driver exec.ExecutorDriver, taskID *mesos.TaskID) {
  	glog.Infof("Kill task")
  }

  func (executor *FreGOExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
  	glog.Infof("Got framework message: %s", msg)
  }

  func (executor *FreGOExecutor) Shutdown(driver exec.ExecutorDriver) {
  	glog.Infof("Shutting down the executor")
  }

  func (executor *FreGOExecutor) Error(driver exec.ExecutorDriver, err string) {
  	glog.Infof("Got error message: %s", err)
  }

  func (executor *FreGOExecutor) processLaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
  	glog.Infof("Launching task %s", taskInfo.GetName())

  	executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_RUNNING, nil)

  	var taskParams *protos.ExecTaskParams
  	if params, err := executor.execTaskParamsEncoder.Unmarshal(taskInfo.Data); err != nil {
  		glog.Errorf("mesos task %s - failed to unmarshal task params; error %v", *taskInfo.TaskId.Value, err)
  		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
  		return
  	} else {
  		taskParams = params.(*protos.ExecTaskParams)
  	}

  	jobID := taskParams.JobId
  	taskID := taskParams.TaskId
  	superstep := taskParams.SuperstepParams.Superstep

  	graph, err := executor.getGraph(taskParams)
  	if err != nil {
  		glog.Errorf("job %s - failed to initialize graph for task %s; error: %v", jobID, taskID, err)
  		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
  		return
  	}
  	defer executor.releaseGraph(taskParams, graph)

  	task, err := NewFreGOTask(taskParams, graph)
  	if err != nil {
  		glog.Errorf("job %s - failed to initialize task %s; error: %v", jobID, taskID, err)
  		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
  		return
  	}

  	superstepResult, err := task.ExecSuperstep(taskParams.SuperstepParams)
  	if err != nil {
  		glog.Errorf("job %s - failed to execute superstep %d for task %s; error %v", jobID, superstep, taskID, err)
  		executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FAILED, nil)
  		return
  	}

  	taskResult := &protos.ExecTaskResult{
  		JobId:           jobID,
  		TaskId:          taskID,
  		SuperstepResult: superstepResult,
  	}

  	executor.sendStatusUpdate(driver, taskInfo.TaskId, mesos.TaskState_TASK_FINISHED, taskResult)
  }

  func (executor *FreG0Executor) sendStatusUpdate(driver exec.ExecutorDriver, taskId *mesos.TaskID,
  	state mesos.TaskState, taskResult *protos.ExecTaskResult) {
  	status := &mesos.TaskStatus{
  		TaskId: taskId,
  		State:  state.Enum(),
  	}

  	if taskResult != nil {
  		data, err := executor.execTaskResultEncoder.Marshal(taskResult)
  		if err != nil {
  			glog.Errorf("job %s - failed to marshal result for task %s", taskResult.JobId, taskResult.TaskId)
  			return
  		}

  		status.Data = data
  	}

  	if _, err := driver.SendStatusUpdate(status); err != nil {
  		glog.Errorf("mesos task %s - error sending status update; error %v", *taskId.Value, err)
  	}
  }

  func (executor *FreGOExecutor) getGraph(params *protos.ExecTaskParams) (*graph.Graph, error) {
  	graphPool, err := executor.getGraphPool(params)
  	if err != nil {
  		return nil, err
  	}

  	prevSuperstep := int(params.SuperstepParams.Superstep) - 1
  	vrange := store.VertexRange(params.SuperstepParams.VertexRange)
  	return graphPool.Get(prevSuperstep, vrange)
  }

  func (executor *FreGOExecutor) releaseGraph(params *protos.ExecTaskParams, graph *graph.Graph) {
  	graphPool, err := executor.getGraphPool(params)
  	if err != nil {
  		glog.Errorf("job %s - failed to return graph to pool; error=%v", params.JobId, err)
  		return
  	}

  	graphPool.Release(graph)
  }

  func (executor *FreGOExecutor) getGraphPool(params *protos.ExecTaskParams) (*graph.Pool, error) {
  	executor.mutex.Lock()
  	defer executor.mutex.Unlock()

  	cacheKey := params.JobId

  	if pool, found := executor.graphPoolCache.Get(cacheKey); found {
  		return pool.(*graph.Pool), nil
  	}

  	pool, err := graph.NewPool(params)
  	if err != nil {
  		return nil, err
  	}

  	executor.graphPoolCache.Set(cacheKey, pool, cache.DefaultExpiration)
  	return pool, nil
  }

  func onGraphPoolEvicted(key string, value interface{}) {
  	pool := value.(*graph.Pool)
  	pool.Close()
  }
