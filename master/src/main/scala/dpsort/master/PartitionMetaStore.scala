package dpsort.master

import scala.collection.mutable
import dpsort.core.storage.PartitionMeta
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{Lock, ReadWriteLock, ReentrantReadWriteLock}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.collection.JavaConverters._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object PartitionMetaStore {

  /*
   * PartitionMetaStore requires strict concurrency control
   */
  private val partitionMetaStore
    = mutable.Map[Int, ListBuffer[PartitionMeta]] ()

  val pmsLock: ReentrantReadWriteLock = new ReentrantReadWriteLock()
  def readlock = pmsLock.readLock()
  def writeLock = pmsLock.writeLock()

  def addPartitionMeta( workerID:Int, pmeta: PartitionMeta ): Unit = {
    writeLock.lock()
    if( ! (partitionMetaStore contains workerID) ) {
      partitionMetaStore.put( workerID, new ListBuffer[PartitionMeta]() )
    }
    partitionMetaStore(workerID).append(pmeta)
    writeLock.unlock()
  }

  def genAndAddPartitionMeta( workerID:Int, pName: String ): Unit = {
    writeLock.lock()
    partitionMetaStore(workerID).append(new PartitionMeta(pName))
    writeLock.unlock()
  }

  def delPartitionMeta( workerId:Int, pName: String ): Unit = {
    writeLock.lock()
    val pMIdx = partitionMetaStore( workerId ).indexWhere( _.pName equals pName )
    partitionMetaStore(workerId).remove(pMIdx)
    writeLock.unlock()
  }

  def getWorkerIds: Iterable[Int] = {
    val iter = partitionMetaStore.keys
    iter
  }

  def getPartitionList(workerID:Int): mutable.ListBuffer[PartitionMeta] = {
    val out = partitionMetaStore(workerID)
    out
  }

  def getRecordsCount: Int = {
    val out = getWorkerIds
      .map( k => partitionMetaStore(k)
        .map( pm => pm.pLines).sum )
      .sum
    out
  }

  override def toString: String = {
    val dataStr:String = getWorkerIds
      .map( k => s"Worker ${k}: ${ partitionMetaStore(k)
                .map( pm => pm.pName + ":" + pm.pLines.toString )
                .mkString(", ") } \n" )
      .foldLeft(""){ _ + _ }
    s"[PartitionMetaStore: \n${dataStr}]"
  }
}
