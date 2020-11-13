package dpsort.master

import dpsort.core.network.MasterTaskServiceGrpc;
import dpsort.core.network.{RegistryMsg, TaskReportMsg, ResponseMsg};
import dpsort.core.network.{ServerInterface, ServerContext}

import scala.concurrent.{ExecutionContext, Future}
import org.apache.logging.log4j.scala.Logging


object MasterTaskServer extends ServerInterface {

  private val port = 0 ; // TODO

  val server : ServerContext = new ServerContext(
    MasterTaskServiceGrpc.bindService(new MasterTaskServiceImpl, ExecutionContext.global),
    "MasterTaskServer",
    port
  )

}

private class MasterTaskServiceImpl extends MasterTaskServiceGrpc.MasterTaskService {
  override def registerWorker(request: RegistryMsg): Future[ResponseMsg] = {
    // TODO context
    Future.successful( new ResponseMsg( ) )
  }

  override def reportTaskResult(request: TaskReportMsg): Future[ResponseMsg] = {
    // TODO context
    // call task result handler
    val response = ResponseMsg()
    Future.successful( response )
  }
}