package dpsort.master

import dpsort.core.execution.RoleContext
import dpsort.master.MasterConf
import org.apache.logging.log4j.scala.Logging

object MasterContext extends RoleContext {

  override def initialize = {
    // Load and instantiate MasterConf
    MasterConf
    // Start networking services
    MasterTaskServer.startServer
    HeartBeatServer.startServer
  }

  override def terminate = {
    MasterTaskServer.stopServer
    HeartBeatServer.stopServer
  }

  override def execute = {
    // Wait until everybody comes in
      // if entry, add IP/Port
            // add files list to partitionmetastore



    // Execute GenBlockStage

    // Execute LocalSortStage

    // Execute SampleKeysStage

    //

    // Terminate

    // r
  }


}
