package dpsort.master

import dpsort.core.execution.{BaseTask, TaskResult, TaskStatus}
import dpsort.core.network.ResponseMsg.ResponseType
import dpsort.core.network.{ChannelMap, ResponseMsg, TaskMsg, TaskReportMsg}
import dpsort.core.utils.SerializationUtils.serializeObjectToByteString
import dpsort.master.execution.Stage
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.Future

object TaskRunner extends Logging{

  private var registeredStage: Stage = null

  def taskResultHandler: TaskReportMsg => Unit = registeredStage.taskResultHandler

  def registerStage( stage: Stage ) : Boolean = {
    if ( registeredStage == null ) {
      registeredStage = stage
      true
    }
    else { false }
  }

  def executeStage: Future[Int] = {
    // submit until terminate condition is satisfied
    while( ! registeredStage.terminateCondition ) {
      for { task <- registeredStage.taskSetIterator } yield {
        if( ! task.isSubmitted ) {
          submitTask( task ) match {
            case true => {
              logger.info(s"task : ${task.getId} submitted to worker ${task.getWorkerID}")
              task.setStatus( TaskStatus.SUBMITTED )
            }
            case false => task.setStatus( TaskStatus.WAITING )
          }
        }
      }
    }

    // unregister stage and terminate
    val result = registeredStage.stageResult
    registeredStage = null
    result match {
      case true  => Future.successful( 0 ) // Exit status 0 implies success
      case false => Future.successful( 1 ) // Other statuses implies failure
    }
  }

  private def submitTask( task: BaseTask ): Boolean = {
    // generate TaskMsg
    val taskMsg = new TaskMsg( serializeObjectToByteString(task) )
    // request
    val reqChannel: TaskReqChannel = ChannelMap
      .getChannel( WorkerMetaStore.getWorkerIpPort(task.getWorkerID) )
      .asInstanceOf[TaskReqChannel]
    // get respond
    val submitResponse: ResponseMsg = reqChannel.requestTask( taskMsg )
    submitResponse.response match {
      case ResponseType.NORMAL => true  // task submitted
      case _ => false                   // task not submitted due to several reasons
    }
  }

}

// it should further check heartbeat and could be able to directly call handler
// to mark all failure.