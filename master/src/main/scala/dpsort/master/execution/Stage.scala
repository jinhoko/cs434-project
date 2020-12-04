package dpsort.master.execution

import dpsort.core.execution.{TaskResult, TaskStatus}
import dpsort.core.network.TaskReportMsg
import dpsort.core.network.TaskReportMsg.TaskResultType
import dpsort.master.TaskRunner
import dpsort.master.TaskRunner._
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object StageExitStatus extends Enumeration {
  val SUCCESS, FAILURE = Value
}

trait Stage extends Logging {
  protected val stageTaskSet: TaskSet = genTaskSet()

  protected def genTaskSet(): TaskSet
  def taskResultHandler( taskRes: TaskReportMsg ): Unit = {
    // override 필요한 놈이 있음 , (sample 경우 필요) -> super.호출하고 그다음 진행
    // TODO if success update PMS
    val taskResultStatus = taskRes.taskResult match {
      case TaskResultType.SUCCESS => TaskStatus.SUCCESS
      case TaskResultType.FAILED  => TaskStatus.FAILURE
    }
    stageTaskSet.findTask( taskRes.taskId ).setStatus( taskResultStatus )
  }
  def toString: String

  def executeAndWaitForTermination(): StageExitStatus.Value = {
    logger.info(s"Register stage ${this.toString}.")
    val registerResult = TaskRunner.registerStage(this )
    if( registerResult == false ) {
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
    // TODO write
    // scan PMS
    // make TaskSet Object and return
  }
}

//class GenBlockStage extends Stage {
//
//  override def toString: String = "GenBlockStage"
//
//  override protected def genTaskSet(): TaskSet = ???
//}
//
