package dpsort.worker

import dpsort.core.execution.BaseTask
import dpsort.core.utils.SerializationUtils
import dpsort.core.network._
import dpsort.core.utils.SerializationUtils.deserializeByteStringToObject
import dpsort.worker.WorkerConf._
import dpsort.core.utils.FileUtils
import dpsort.worker.utils.PartitionUtils._


import scala.concurrent.{ExecutionContext, Future}
import org.apache.logging.log4j.scala.Logging

object WorkerShuffleServer extends ServerInterface {

  private val port = get("dpsort.worker.shufflePort").toInt

  val server : ServerContext = new ServerContext(
    ShuffleServiceGrpc.bindService(new ShuffleServiceImpl, ExecutionContext.global),
    "WorkerShuffleServer",
    port
  )

}

private class ShuffleServiceImpl extends ShuffleServiceGrpc.ShuffleService with Logging {

  override def requestShuffle(request: ShuffleRequestMsg): Future[ResponseMsg] = {
    logger.debug(s"shuffle request arrived")
    val response = ShuffleManager.shuffleRequestHandler
    Future.successful( response )
  }

  override def sendShuffleData(request: ShuffleDataMsg): Future[ResponseMsg] = {
    val shuffleData: Array[Array[Byte]] =
      deserializeByteStringToObject(request.serializedShuffleData).asInstanceOf[Array[Array[Byte]]]
    val shufflePartName: String =
      deserializeByteStringToObject(request.serializedPartitionName).asInstanceOf[String]
    logger.debug(s"shuffle data arrived, name: ${shufflePartName}")

    FileUtils.writeLinesToFile( shuffleData, getPartitionPath(shufflePartName) )
    ShuffleManager.shuffleReceiveLock.lock()
    ShuffleManager.numOngoingReceiveShuffles -= 1
    ShuffleManager.shuffleReceiveLock.unlock()

    logger.debug(s"arrived data write finished, name: ${shufflePartName}")
    val response: ResponseMsg = new ResponseMsg( ResponseMsg.ResponseType.NORMAL )
    Future.successful( response )
  }

}