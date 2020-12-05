package dpsort.worker

import dpsort.worker.WorkerConf._
import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor}

import dpsort.core.execution.{BaseTask, TaskType}
import dpsort.core.network.{ChannelMap, ResponseMsg, TaskReportMsg}
import dpsort.core.network.ResponseMsg._
import dpsort.core.utils.SerializationUtils._
import dpsort.core.network.TaskReportMsg.TaskResultType
import org.apache.logging.log4j.scala.Logging


object TaskManager extends Logging {

  private val executor: ExecutorService = Executors.newFixedThreadPool( get("dpsort.worker.threads").toInt )

  var terminationFlag: Boolean = false
  var numRunningThreads = 0

  private val numThreads = get("dpsort.worker.threads").toInt
  private def numIdleThreads = numThreads - numRunningThreads

  private def isTaskSubmitAvailable: Boolean = numIdleThreads > 0

  def taskRequestHandler(task: BaseTask): ResponseMsg = {
    logger.debug(s"task request arrived")
    if( isTaskSubmitAvailable ) {
      logger.debug(s"task execution available")
      numRunningThreads += 1
      executor.execute( new TaskExecutionContext(task) )
      new ResponseMsg( ResponseType.NORMAL )
    }
    else {
      logger.debug(s"task execution unavailable")
      new ResponseMsg( ResponseType.HANDLE_ERROR )
    }
  }

  def terminationContext(): Unit = {
    executor.shutdown()
    logger.info("executor terminated")
  }

  def taskManagerContext(): Unit = {
    while( true ) {
      logger.info(s"status report : ${numRunningThreads} task(s) are running")
      Thread.sleep( 3000 )
      if( terminationFlag ) {
        logger.info( s"executor termination flag is set. now terminating" )
        terminationContext
        return
      }
    }
  }

}

class TaskExecutionContext( task: BaseTask ) extends Runnable with Logging {
  override def run(): Unit = {
    logger.debug(s"task ${task.getId} execution started.")
    Thread.sleep(1000 )
    task.run
    logger.info(s"task ${task.getId} execution finished. now reporting result")
    val reqChannel: MasterReqChannel = ChannelMap.getChannel(WorkerParams.MASTER_IP_PORT)
      .asInstanceOf[MasterReqChannel]
    val reportResponse: ResponseMsg = reqChannel.reportTaskResult(
      new TaskReportMsg( taskId = task.getId, taskResult = TaskResultType.SUCCESS, serializedTaskResultData = getEmptyByteString  )
    )
    // TODO further consider possibility that report might fail
    // TODO further consider possibility that task might fail

    TaskManager.numRunningThreads -= 1
    if( task.getTaskType == TaskType.TERMINATETASK ){
      TaskManager.terminationFlag = true
    }
  }

}