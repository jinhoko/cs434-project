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
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import java.util.concurrent.locks.Lock

import org.apache.logging.log4j.scala.Logging

import dpsort.core.{MAX_KEY, MutablePartFunc, PartFunc}
import dpsort.core.execution._
import dpsort.core.execution.Role
import dpsort.core.utils.SortUtils
import dpsort.master.execution._


object MasterContext extends Role with Logging {

  val registryLock = new java.util.concurrent.locks.ReentrantLock

  var lastStageExitStatus: StageExitStatus.Value = StageExitStatus.SUCCESS
  var recordsCount: Int = 0
  var sampledKeys: mutable.ArrayBuffer[Array[Byte]] = mutable.ArrayBuffer[Array[Byte]]()
  var partitionFunction: PartFunc = Array[(Array[Byte], (String, Int))]()

  override def initialize = {
    // Start networking services
    MasterTaskServer.startServer
  }

  override def terminate = {
    MasterTaskServer.stopServer
  }

  override def execute = {

    logger.info(s"waiting for workers")
    val workerRegistryWaitCondition = Future {
      while( WorkerMetaStore.getWaitingWorkersNum > 0 ) {
        Thread.sleep(3000)
      }
      registryLock.lock()
      registryLock.unlock()
    }
    val workerRegistryWait = Await.result( workerRegistryWaitCondition , Duration.Inf )
    logger.info(s"all ${MasterParams.NUM_SLAVES_INT} workers registered")
    recordsCount = PartitionMetaStore.getRecordsCount
    println(s"${PartitionMetaStore.toString}")

    /* Execute Stages */

    val stage0 = new GenBlockStage
    lastStageExitStatus = stage0.executeAndWaitForTermination()
    println(s"${PartitionMetaStore.toString}")

    val stage1 = new LocalSortStage
    lastStageExitStatus = stage1.executeAndWaitForTermination()
    println(s"${PartitionMetaStore.toString}")

    val stage2 = new SampleKeyStage
    lastStageExitStatus = stage2.executeAndWaitForTermination()
    genPartitionFunction

    val stage3 = new PartitionAndShuffleStage
    lastStageExitStatus = stage3.executeAndWaitForTermination()
    println(s"${PartitionMetaStore.toString}")

    while( ! isMergeFinished ) {
      val stage4 = new MergeStage
      lastStageExitStatus = stage4.executeAndWaitForTermination()
      println(s"${PartitionMetaStore.toString}")
    }

    val stageLast = new TerminateStage
    lastStageExitStatus = stageLast.executeAndWaitForTermination()

  }

  private def genPartitionFunction(): Unit = {
    /*
     * Here our partitioning policy
     * equally assigns records all workers,
     * without considering the size of each worker's input files
     */
    val wNum = WorkerMetaStore.getWorkerNum
    assert( sampledKeys.size >= wNum )

    val keysArr = sampledKeys.toArray
    SortUtils.sortLines( keysArr )

    def slidingSize = ( keysArr.size.toFloat / wNum.toFloat ).toInt
    val pivots = (1 to wNum).map( idx => {    // Key less or equal than pivot will be assigned to that slot
      if ( idx == wNum ) { MAX_KEY }
      else { keysArr( idx * slidingSize ) } }
    )
    val shuffleInfo = PartitionMetaStore.getWorkerIds
      .map( id => WorkerMetaStore.getWorkerShuffleIPPort(id) )

    logger.info("printing partition pivots : ")
    pivots.foreach( a => logger.info(s"> pivot : ${a.map(_.toChar).mkString}") )
    assert( pivots.size == shuffleInfo.size )
    val pFunc = pivots.zip( shuffleInfo ).toArray
    MasterContext.partitionFunction = pFunc
  }

  private def isMergeFinished: Boolean = {
    PartitionMetaStore.getWorkerIds.map(
      wid => PartitionMetaStore.getPartitionList(wid).size <= 1
    ).reduce( (b1, b2) => b1 && b2 )
  }
}
