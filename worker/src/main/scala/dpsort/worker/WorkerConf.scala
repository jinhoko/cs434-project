package dpsort.worker

import dpsort.core.ConfContext

object WorkerConf extends ConfContext {

  // Load preconfigured properties for worker.
  // The .properties file must be placed in worker/src/main/resources
  loadFromResourceProperties("/worker-conf-default.properties")

}
