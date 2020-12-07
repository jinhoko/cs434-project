package dpsort.worker

import dpsort.core.execution.BaseTask
import dpsort.core.utils.SerializationUtils
import dpsort.core.network._
import dpsort.core.utils.SerializationUtils.deserializeByteStringToObject
import dpsort.worker.WorkerConf._

import scala.concurrent.{ExecutionContext, Future}
import org.apache.logging.log4j.scala.Logging

object WorkerShuffleServer extends ServerInterface {

  private val port = get("dpsort.worker.shufflePort").toInt

  val server : ServerContext = new ServerContext(
    WorkerTaskServiceGrpc.bindService(new WorkerTaskServiceImpl, ExecutionContext.global),
    "WorkerShuffleServer",
    port
  )

}

private class ShuffleServiceImpl extends ShuffleServiceGrpc.ShuffleService with Logging {

  override def requestShuffle(request: ShuffleRequestMsg): Future[ResponseMsg] = {
    // TODO pass to handler 4
    // TODO write response
    val response: ResponseMsg = new ResponseMsg()
    Future.successful( response )
  }

  override def sendShuffleData(request: ShuffleDataMsg): Future[ResponseMsg] = {
    // write directly to file
    // numOngoingShuffles -= 1
    // TODO write response
    val response: ResponseMsg = new ResponseMsg()
    Future.successful( response )
  }

}