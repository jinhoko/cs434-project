package dpsort.master

import scala.collection.mutable
import dpsort.core.storage.PartitionMeta

object PartitionMetaStore {

  private var partitionMetaStore: mutable.Map[Int, mutable.ArrayBuffer[PartitionMeta] ]
    = mutable.Map[Int, mutable.ArrayBuffer[PartitionMeta] ]()

  def addPartitionMeta( workerID:Int, pmeta: PartitionMeta ): Unit = {
    if( ! (partitionMetaStore contains workerID) ) {
      partitionMetaStore += (workerID -> new mutable.ArrayBuffer[PartitionMeta]() )
    }
    partitionMetaStore(workerID).append( pmeta )
  }

  def getWorkerIds( ): Iterable[Int] = {
    partitionMetaStore.keys
  }

  // TODO querying functions

  override def toString: String = {
    val dataStr = partitionMetaStore.keys
      .map( k => s"Worker ${k}: ${partitionMetaStore(k).map(_.pName).mkString(", ")} \n" ).foldLeft(""){ _ + _ }
    s"[PartitionMetaStore: ${dataStr}]"
  }
}
