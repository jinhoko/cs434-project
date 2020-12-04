package dpsort.master

import com.google.protobuf.ByteString
import dpsort.core.network.MasterTaskServiceGrpc
import dpsort.core.network.{RegistryMsg, ResponseMsg, TaskReportMsg}
import dpsort.core.network.{ServerContext, ServerInterface}
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
    if ( ! isDistinctRegistry( registry ) ) {
      logger.info(s"Worker registry from ${registry.IP}:${registry.PORT} failed.")
      Future.successful( new ResponseMsg( ResponseMsg.ResponseType.HANDLE_ERROR ))
    }
    else {
      // Register object
      registry._WORKER_ID = WorkerMetaStore.addRegistry( registry )
      registry.INPUT_FILES.foreach( addPartitionMeta(registry._WORKER_ID, _) )
      logger.info(s"Worker id: ${ WorkerMetaStore.getWorkerNum } from ${registry.IP_PORT} registered. " +
        s"${  WorkerMetaStore.getWaitingWorkersNum } remaining.")
      Future.successful( new ResponseMsg( ResponseMsg.ResponseType.NORMAL ) )
    }
  }

  override def reportTaskResult(request: TaskReportMsg): Future[ResponseMsg] = {
    // TODO context
    // call task result handler
    val response = ResponseMsg()
    Future.successful( response )
  }
}