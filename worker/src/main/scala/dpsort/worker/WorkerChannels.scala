package dpsort.worker

import dpsort.worker.WorkerParams
import dpsort.core.network.{Channel, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, TaskReportMsg}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import org.apache.logging.log4j.scala.Logging

class MasterReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val masterTaskChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val masterTaskBlockingStub = MasterTaskServiceGrpc.blockingStub( masterTaskChannel )

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

//class ShuffleReqChannel( ipPort: (String, Int) ) extends Channel with Logging {
//
//  override def channel: Any = ???
//  override val stub: Any = _
//  override def request: Any = ???
//}


//
//  def shutdown(): Unit = {
//    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
//  }