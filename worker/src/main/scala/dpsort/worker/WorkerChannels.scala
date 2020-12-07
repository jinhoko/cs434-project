package dpsort.worker

import dpsort.core.network._
import dpsort.worker.WorkerParams
import dpsort.core.network.{Channel, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, ShuffleDataMsg, ShuffleRequestMsg, TaskReportMsg}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global


class MasterReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val masterTaskChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val masterTaskBlockingStub = MasterTaskServiceGrpc.blockingStub( masterTaskChannel )

  /* Blocking request */
  def registerWorker(request: RegistryMsg): ResponseMsg = {
    try {
      val response = masterTaskBlockingStub.registerWorker(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"RPC failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

  /* Blocking request */
  def reportTaskResult( request: TaskReportMsg ): ResponseMsg = {
    try {
      val response = masterTaskBlockingStub.reportTaskResult(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Task report failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }
}

class ShuffleReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val shuffleChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val shuffleBlockingStub = ShuffleServiceGrpc.blockingStub( shuffleChannel )
  private val shuffleNonBlockingStub = ShuffleServiceGrpc.stub( shuffleChannel )

  /* Blocking request */
  def requestShuffle( request: ShuffleRequestMsg ): ResponseMsg = {
    try {
      val response = shuffleBlockingStub.requestShuffle(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Shuffle request failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

  /* Non-Blocking request */
  def sendShuffleData( request: ShuffleDataMsg ): Future[ResponseMsg] = {
    try {
      val response: Future[ResponseMsg] = shuffleNonBlockingStub.sendShuffleData(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Shuffle data transmission failed: ${e.getStatus.toString}")
        throw e
    }
  }
}