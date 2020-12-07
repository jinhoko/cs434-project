package dpsort.master

import scala.collection.mutable
import dpsort.core.storage.PartitionMeta
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

object PartitionMetaStore {

  private var partitionMetaStore: ConcurrentHashMap[Int, ListBuffer[PartitionMeta]]
  = new ConcurrentHashMap[Int, ListBuffer[PartitionMeta]] ()

  def execute(body: => Unit) = ExecutionContext.global.execute( new Runnable {
    def run() = body
  })

  def addPartitionMeta( workerID:Int, pmeta: PartitionMeta ): Unit = {
    if( ! (partitionMetaStore contains workerID) ) {
      partitionMetaStore.put( workerID, new ListBuffer[PartitionMeta]() )
    }
    partitionMetaStore.get( workerID ).append( pmeta )
  }

  def genAndAddPartitionMeta( workerID:Int, pName: String ): Unit = {
    partitionMetaStore.get(workerID).append(new PartitionMeta(pName))
  }

  def delPartitionMeta( workerId:Int, pName: String ): Unit = {
    val pMIdx = partitionMetaStore.get( workerId ).indexWhere( _.pName equals pName )
    partitionMetaStore.get( workerId ).remove( pMIdx )
  }

  def getWorkerIds( ): Iterable[Int] = { partitionMetaStore.keySet().asScala.toIterable }

  def getPartitionList(workerID:Int): mutable.ListBuffer[PartitionMeta] = {
    partitionMetaStore.get(workerID)
  }

  override def toString: String = {
    val dataStr:String = getWorkerIds()
      .map( k => s"Worker ${k}: ${ partitionMetaStore.get(k)
                .map( pm => pm.pName + ":" + pm.pLines.toString )
                .mkString(", ") } \n" )
      .foldLeft(""){ _ + _ }
    s"[PartitionMetaStore: \n${dataStr}]"
  }
}
