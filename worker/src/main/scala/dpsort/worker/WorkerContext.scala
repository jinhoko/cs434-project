package dpsort.worker

import dpsort.core.Registry
import dpsort.core.execution.RoleContext
import dpsort.core.network.RegistryMsg
import dpsort.core.utils.SerializationUtils._
import dpsort.worker.WorkerConf._
import org.apache.logging.log4j.scala.Logging

object WorkerContext extends RoleContext {

  override def initialize: Unit = {
    // Start networking services
    WorkerTaskServer.startServer
    // Open worker channels
    WorkerChannels
    // Start taskmanager threads TODO
  }

  override def terminate: Unit = {
    // Stop task server
    WorkerTaskServer.stopServer
  }

  override def execute: Unit = {
    // Start shuffle channel TODO

    // Register worker to master
    val registryResponse = WorkerChannels.registerWorker( genRegistry )
      // TODO we can only proceed when return value is valid.
      // if registryResponse.response.value == ??? return

    // Start heartbeat channel TODO

    // Start taskmonitor TODO

  }

  private def genRegistry() = {
    val registryObj = new Registry(
      get("dpsort.worker.ip"),
      get("dpsort.worker.port").toInt,
      WorkerParams.INPUT_FILES_STRARR
    )
    new RegistryMsg( serializeObjectToByteString( registryObj ) )
  }
}
