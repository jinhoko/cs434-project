/*
 * MIT License
 *
 * Copyright (c) 2020 Jinho Ko
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package dpsort.master

import scala.concurrent.{ExecutionContext, Future}

import org.apache.logging.log4j.scala.Logging
import com.google.protobuf.ByteString

import dpsort.core.network.{ChannelMap, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, ServerContext, ServerInterface, TaskReportMsg}
import dpsort.core.Registry
import dpsort.core.storage.PartitionMeta
import dpsort.core.utils.IdUtils
import dpsort.core.utils.SerializationUtils._
import dpsort.master.MasterConf._
import dpsort.master.PartitionMetaStore._
import dpsort.master.WorkerMetaStore._


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
      MasterContext.registryLock.lock()

      registry._WORKER_ID = WorkerMetaStore.addRegistry( registry )
      registry.INPUT_FILES.foreach( addPartitionMeta(registry._WORKER_ID, _) )
      logger.info(s"Worker id: ${ WorkerMetaStore.getWorkerNum } from ${registry.IP_PORT} registered. " +
        s"${  WorkerMetaStore.getWaitingWorkersNum } remaining.")
      ChannelMap.addChannel( registry.IP_PORT, new TaskReqChannel( registry.IP_PORT ))
      MasterContext.registryLock.unlock()

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