package dpsort.core.storage

import dpsort.core.utils.FileUtils

object PartitionMeta {
}

@SerialVersionUID(2000L)
final class PartitionMeta( pn:String ) extends Serializable {

  /* NOTE : pName can be be in both abs/rel path
   *        abs) absolute filepath
   *        rel) directory is dpsort.worker.workdir
   */
  val pName: String = pn
  val pLines: Int = {
    if( FileUtils.checkIfFileExists( pName ) ) {
      FileUtils.getNumLinesInFile( pName )
    } else {
      0
    }
  }
}