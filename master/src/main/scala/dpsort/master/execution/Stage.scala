package dpsort.master.execution

import dpsort.core.execution._
import dpsort.core.network.TaskReportMsg
import dpsort.core.network.TaskReportMsg.TaskResultType
import dpsort.core.utils.IdUtils
import dpsort.master.{PartitionMetaStore, TaskRunner}
import dpsort.master.TaskRunner._
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

object StageExitStatus extends Enumeration {
  val SUCCESS, FAILURE = Value
}

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
    // override 필요한 놈이 있음 , (sample 경우 필요) -> super.호출하고 그다음 진행
    // TODO if success update PMS
    val taskResultStatus = taskRes.taskResult match {
      case TaskResultType.SUCCESS => TaskStatus.SUCCESS
      case TaskResultType.FAILED  => TaskStatus.FAILURE
    }
    stageTaskSet.getTask( taskRes.taskId ).setStatus( taskResultStatus )
  }

  def executeAndWaitForTermination(): StageExitStatus.Value = {
    logger.info(s"Register stage ${this.toString}.")
    val registerResult = TaskRunner.registerStage(this )
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

class EmptyStage extends Stage {
  override def toString: String = "EmptyStage"
  override protected def genTaskSet(): TaskSet = {
    val taskSeq: Iterable[BaseTask] = {
      for (i <- 0 to 9;
           wid <- PartitionMetaStore.getWorkerIds)
        yield new EmptyTask(IdUtils.genNewTaskID, wid, TaskStatus.WAITING, Unit, Unit)
    }
    logger.info(s"${taskSeq.size} task(s) generated")
    new TaskSet( Random.shuffle( taskSeq ) ) // for fair scheduling
  }
}

class TerminateStage extends Stage {
  override def toString: String = "TerminationStage"
  override protected def genTaskSet(): TaskSet = {
    val taskSeq: Iterable[BaseTask] = {
      for ( wid <- PartitionMetaStore.getWorkerIds ) // TODO need to generate task for all partitions
        yield new TerminateTask(IdUtils.genNewTaskID, wid, TaskStatus.WAITING, Unit, Unit)
    }
    new TaskSet( Random.shuffle( taskSeq ) ) // for fair scheduling
  }
}

//class GenBlockStage extends Stage {
//
//  override def toString: String = "GenBlockStage"
//
//  override protected def genTaskSet(): TaskSet = ???
//}
//
