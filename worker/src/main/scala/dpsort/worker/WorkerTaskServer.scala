package dpsort.worker

import dpsort.core.network.{ResponseMsg, ServerContext, ServerInterface, TaskMsg, WorkerTaskServiceGrpc}
import dpsort.worker.WorkerConf._

import scala.concurrent.{ExecutionContext, Future}
import org.apache.logging.log4j.scala.Logging


object WorkerTaskServer extends ServerInterface {

  private val port = get("dpsort.worker.port").toInt

  val server : ServerContext = new ServerContext(
    WorkerTaskServiceGrpc.bindService(new WorkerTaskServiceImpl, ExecutionContext.global),
    "WorkerTaskServer",
    port
  )

}

private class WorkerTaskServiceImpl extends WorkerTaskServiceGrpc.WorkerTaskService {

  override def requestTask(request: TaskMsg): Future[ResponseMsg] = {
    // TODO context
    Future.successful( new ResponseMsg( ) )
  }

}