package dpsort.worker

import dpsort.worker.WorkerParams
import dpsort.core.network.MasterTaskServiceGrpc
import dpsort.core.network.{RegistryMsg, ResponseMsg}

import io.grpc.{StatusRuntimeException, ManagedChannelBuilder, ManagedChannel}

import org.apache.logging.log4j.scala.Logging

object WorkerChannels extends Logging {

  val masterTaskChannel = ManagedChannelBuilder
    .forAddress(WorkerParams.MASTER_IP_STR, WorkerParams.MASTER_PORT_INT)
    .usePlaintext.build

  val masterTaskBlockingStub = MasterTaskServiceGrpc.blockingStub(masterTaskChannel)

  def registerWorker(request: RegistryMsg): ResponseMsg = {
    try {
      val response = masterTaskBlockingStub.registerWorker(request)
      response
    } catch { // TODO current failure policy too strict. needs relaxation
      case e: StatusRuntimeException =>
        logger.error(s"RPC failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

  // TODO heartbeatchannel


}

// TODO channel shutdown?
//
//  def shutdown(): Unit = {
//    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
//  }