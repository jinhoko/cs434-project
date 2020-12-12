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

package dpsort.master.execution

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.apache.logging.log4j.scala.Logging

import dpsort.core.execution._
import dpsort.core.network.TaskReportMsg
import dpsort.core.network.TaskReportMsg.TaskResultType
import dpsort.master.{MasterContext, PartitionMetaStore, TaskManager}
import dpsort.master.MasterConf._
import dpsort.master.TaskManager._


object StageExitStatus extends Enumeration {
  val SUCCESS, FAILURE = Value
}

/*
 * With the indent to make workers stupid,
 * the stage is hidden to the worker.
 * Worker only executes TaskContexts.
 */
trait Stage extends Logging {

  protected val stageTaskSet: TaskSet = genTaskSet()
  logger.info(s"taskset generated with ${stageTaskSet.getNumTasks} task(s)")

  protected def genTaskSet(): TaskSet

  def taskSet: Set[BaseTask] = stageTaskSet.taskSet

  def terminateCondition: Boolean = stageTaskSet.isAllTasksFinished
  def stageResult: Boolean = stageTaskSet.isAllTasksSucceeded
  def numFinishedTasks: Int = stageTaskSet.getNumFinishedTasks
  def numRemainingTasks: Int = stageTaskSet.getNumRemainingTasks

  def toString: String

  def taskResultHandler( taskRes: TaskReportMsg ): Unit = {
    val taskResultStatus = taskRes.taskResult match {
      case TaskResultType.SUCCESS => TaskStatus.SUCCESS
      case TaskResultType.FAILED  => {
        logger.error(s"task ${taskRes.taskId.toString} failed!")
        TaskStatus.FAILURE
      }
    }
    stageTaskSet.getTask( taskRes.taskId ).setStatus( taskResultStatus )
  }

  def executeAndWaitForTermination(): StageExitStatus.Value = {
    logger.info(s"Register stage ${this.toString}.")
    val registerResult = TaskManager.registerStage(this )
    if( !registerResult ) {
      logger.error(s"Cannot register stage to TaskRunner")
      return StageExitStatus.FAILURE
    }
    logger.info(s"Executing stage ${this.toString}...")

    val stageExitCode: Int = Await.result( executeStage, Duration.Inf )

    logger.info(s"Stage ${this.toString} exited with code ${stageExitCode}")
    val stageExitStatus = stageExitCode match  {
      case 0 => {
        logger.info(s"Stage ${this.toString} finally marked success.")
        StageExitStatus.SUCCESS
      }
      case _ => {
        logger.error(s"Stage ${this.toString} finally marked failure.")
        StageExitStatus.FAILURE
      }
    }
    stageExitStatus
  }
}