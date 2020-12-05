package dpsort.master

import com.google.protobuf.ByteString
import dpsort.core.network.{ChannelMap, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, ServerContext, ServerInterface, TaskReportMsg}
import dpsort.core.Registry
import dpsort.core.storage.PartitionMeta
import dpsort.core.utils.IdUtils
import dpsort.core.utils.SerializationUtils._
import dpsort.master.MasterConf._
import dpsort.master.PartitionMetaStore._
import dpsort.master.WorkerMetaStore._

import scala.concurrent.{ExecutionContext, Future}
import org.apache.logging.log4j.scala.Logging


object MasterTaskServer extends ServerInterface {

  private val port = get("dpsort.master.port").toInt

  val server : ServerContext = new ServerContext(
    MasterTaskServiceGrpc.bindService(new MasterTaskServiceImpl, ExecutionContext.global),
    "MasterTaskServer",
    port
  )

}

private class MasterTaskServiceImpl extends MasterTaskServiceGrpc.MasterTaskService with Logging {
  override def registerWorker(request: RegistryMsg): Future[ResponseMsg] = {

    val bytestr: ByteString = request.serializedRegistryObject
    val registry: Registry = deserializeByteStringToObject[Registry](bytestr)
    if ( isDistinctRegistry( registry ) ) {
      // Register object
      registry._WORKER_ID = WorkerMetaStore.addRegistry( registry )
      registry.INPUT_FILES.foreach( addPartitionMeta(registry._WORKER_ID, _) )
      logger.info(s"Worker id: ${ WorkerMetaStore.getWorkerNum } from ${registry.IP_PORT} registered. " +
        s"${  WorkerMetaStore.getWaitingWorkersNum } remaining.")
      ChannelMap.addChannel( registry.IP_PORT, new TaskReqChannel( registry.IP_PORT ))
      Future.successful( new ResponseMsg( ResponseMsg.ResponseType.NORMAL ) )
    }
    else {
      logger.info(s"Worker registry from ${registry.IP}:${registry.PORT} failed.")
      Future.successful( new ResponseMsg( ResponseMsg.ResponseType.HANDLE_ERROR ))
    }
  }

  override def reportTaskResult(request: TaskReportMsg): Future[ResponseMsg] = {
    logger.debug(s"Task report arrived")
    TaskRunner.taskResultHandler( request )
    val response = ResponseMsg( ResponseMsg.ResponseType.NORMAL )
    Future.successful( response )
  }
}