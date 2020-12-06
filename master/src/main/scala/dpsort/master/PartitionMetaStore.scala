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

  def genAndAddPartitionMeta( workerID:Int, pName: String ): Unit = {
    partitionMetaStore(workerID).append( new PartitionMeta( pName ) )
  }

  def delPartitionMeta( workerId:Int, pName: String ): Unit = {
    val pMIdx = partitionMetaStore( workerId ).indexWhere( _.pName == pName )
    partitionMetaStore( workerId ).remove( pMIdx )
  }

  def getWorkerIds( ): Iterable[Int] = { partitionMetaStore.keys }

  def getPartitionList(workerID:Int): mutable.ArrayBuffer[PartitionMeta] = {
    partitionMetaStore( workerID )
  }

  override def toString: String = {
    val dataStr = partitionMetaStore.keys
      .map( k => s"Worker ${k}: ${partitionMetaStore(k).map( pm => pm.pName + ":" + pm.pLines.toString ).mkString(", ")} \n" ).foldLeft(""){ _ + _ }
    s"[PartitionMetaStore: \n${dataStr}]"
  }
}
