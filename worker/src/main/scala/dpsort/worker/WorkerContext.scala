package dpsort.worker

import dpsort.core.Registry
import dpsort.core.execution.Role
import dpsort.core.network.{RegistryMsg, ResponseMsg}
import dpsort.core.utils.SerializationUtils._
import dpsort.worker.WorkerConf._
import dpsort.core.storage.PartitionMeta
import dpsort.core.utils.FileUtils
import javax.xml.crypto.dsig.spec.ExcC14NParameterSpec
import org.apache.logging.log4j.scala.Logging

object WorkerContext extends Role {

  override def initialize: Unit = {
    // Start networking services
    WorkerTaskServer.startServer
    // Open worker channels
    WorkerChannels
    // Start taskmanager threads TODO

    // Set working directory
    initWorkDir
  }

  override def terminate: Unit = {
    // Stop task server
    WorkerTaskServer.stopServer
  }

  override def execute: Unit = {
    // Start shuffle channel TODO

    // Register worker to master
    val registryResponse: ResponseMsg = WorkerChannels.registerWorker(genRegistry)
    if( registryResponse != ResponseMsg.ResponseType.NORMAL ) { return; }

    // Start heartbeat channel TODO

    // Start taskmonitor TODO

  }

  private def genRegistry() = {
    val registryObj = new Registry(
      WorkerConf.get("dpsort.worker.ip"),
      WorkerConf.get("dpsort.worker.port").toInt,
      WorkerParams.INPUT_FILES_STRARR.map( new PartitionMeta( _ ) )
    )
    new RegistryMsg( serializeObjectToByteString( registryObj ) )
  }

  private def initWorkDir(): Unit = {
    FileUtils.makeDirectory( "tmp" )
    FileUtils.createAndClearDirectory( get("dpwort.worker.workdir") )
  }
}
