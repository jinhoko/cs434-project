/*
 * MIT License
 *
 * Copyright (c) 2020 Jinho Ko
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package dpsort.worker

import dpsort.worker.WorkerConf._
import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor}

import com.google.protobuf.ByteString
import dpsort.core.execution.{BaseTask, TaskType}
import dpsort.core.network.{ChannelMap, ResponseMsg, TaskReportMsg}
import dpsort.core.network.ResponseMsg._
import dpsort.core.utils.SerializationUtils._
import dpsort.core.network.TaskReportMsg.TaskResultType
import dpsort.worker
import dpsort.worker.execution.ExecCtxtFetcher
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
      executor.execute( new TaskExecutionRunnable(task) ) // TODO change here.
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

class TaskExecutionRunnable(task: BaseTask ) extends Runnable with Logging {
  override def run(): Unit = {
    logger.debug(s"task ${task.getId} execution started.")
    Thread.sleep(1000 )

    val taskOutput: Either[Unit, ByteString] = ExecCtxtFetcher.getContext(task).run(task)
    val resultData = taskOutput match {
      case Left(value) => getEmptyByteString
      case Right(value) => value
    }

    logger.info(s"task ${task.getId} execution finished. now reporting result")
    val reqChannel: MasterReqChannel = ChannelMap.getChannel(WorkerParams.MASTER_IP_PORT)
      .asInstanceOf[MasterReqChannel]
    val reportResponse: ResponseMsg = reqChannel.reportTaskResult(
      new TaskReportMsg( taskId = task.getId, taskResult = TaskResultType.SUCCESS, serializedTaskResultData = resultData  )
    )
    // TODO further consider possibility that report might fail
      // execute 말고 submit으로 해서 onfailure 처리해야됨. 지금은 fail하면 그냥 보고를 안함.
    // TODO further consider possibility that task might fail

    TaskManager.numRunningThreads -= 1
    if( task.getTaskType == TaskType.TERMINATETASK ){
      TaskManager.terminationFlag = true
    }
  }

}