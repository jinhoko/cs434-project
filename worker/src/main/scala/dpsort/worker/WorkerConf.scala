package dpsort.worker

import dpsort.core.ConfContext
import dpsort.core.utils.FileUtils.{checkIfFileExists, getAbsPath}

object WorkerConf extends ConfContext {

  // Load preconfigured properties for worker.
  // The .properties file must be placed in worker/src/main/resources
  loadFromResourceProperties("/worker-conf-default.properties")
  val userConfFileDir = getAbsPath( "conf/worker-conf.properties" )
  if( checkIfFileExists( userConfFileDir ) ){
    loadFromUserDefinedProperties( userConfFileDir )
  }

}
