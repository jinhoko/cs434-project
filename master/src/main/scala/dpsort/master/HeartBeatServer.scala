package dpsort.master

import dpsort.core.network.HeartBeatServiceGrpc;
import dpsort.core.network.{HeartBeatMsg, ResponseMsg};
import dpsort.core.network.{ServerInterface, ServerContext}

import scala.concurrent.{ExecutionContext, Future}
import org.apache.logging.log4j.scala.Logging


object HeartBeatServer extends ServerInterface {

  private val port = 0 ; // TODO

  val server : ServerContext = new ServerContext(
    HeartBeatServiceGrpc.bindService(new HeartBeatServiceImpl, ExecutionContext.global),
    "HeartBeatServer",
    port
  )

}


private class HeartBeatServiceImpl extends HeartBeatServiceGrpc.HeartBeatService {
  override def heartBeatReport(request: HeartBeatMsg): Future[ResponseMsg] = {
    // TODO context
    val response = ResponseMsg()
    Future.successful( response )
  }
}