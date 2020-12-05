package dpsort.master

import dpsort.core.network.{Channel, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, TaskMsg, WorkerTaskServiceGrpc}
import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import org.apache.logging.log4j.scala.Logging

class TaskReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val taskReqChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val workerTaskBlockingStub = WorkerTaskServiceGrpc.blockingStub( taskReqChannel)

  def requestTask(request: TaskMsg): ResponseMsg = {
    try {
      val response = workerTaskBlockingStub.requestTask(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Task Request failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

}
