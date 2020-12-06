package dpsort.worker.wUtils

import dpsort.core.utils.FileUtils._
import dpsort.worker.WorkerConf._

object PartitionUtils {

  def getPartitionPath( partName: String ) = {
    getAbsPath( get("dpsort.worker.workdir") ) + "/" + partName
  }

}
