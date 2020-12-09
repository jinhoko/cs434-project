/*
 * MIT License
 *
 * Copyright (c) 2020 Jinho Ko
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package dpsort.master

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{Lock, ReadWriteLock, ReentrantReadWriteLock}

import ExecutionContext.Implicits.global

import dpsort.core.storage.PartitionMeta


object PartitionMetaStore {

  /*
   * PartitionMetaStore requires strict concurrency control
   */
  private val partitionMetaStore
    = mutable.SortedMap[Int, ListBuffer[PartitionMeta]] ()

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
