package dpsort.master

import dpsort.core.execution.{BaseTask, TaskResult}
import dpsort.core.network.TaskReportMsg
import dpsort.master.execution.Stage

import scala.concurrent.Future

object TaskRunner {

  private var registeredStage: Stage = null

  def taskResultHandler: TaskReportMsg => Unit = registeredStage.taskResultHandler

  def registerStage( stage: Stage ) : Boolean = {
    if ( registeredStage == null ) {
      registeredStage = stage
      true
    }
    else { false }
  }

  def executeStage() : Future[Int] = {
    // TODO lots of context



    // unregister stage
    registeredStage = null
    Future.successful( 0 ) // TODO change code
  }

}

// it should further check heartbeat and could be able to directly call handler
// to mark all failure.