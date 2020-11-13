package dpsort.master

import dpsort.core.ConfContext

object MasterConf extends ConfContext {

  // Load preconfigured properties for master.
  // The .properties file must be placed in master/src/main/resources
  loadFromResourceProperties("/master-conf-default.properties")

}
