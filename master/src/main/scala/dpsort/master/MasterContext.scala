package dpsort.master

import java.util.concurrent.locks.Lock

import dpsort.core.MAX_KEY
import dpsort.core.execution.Role
import dpsort.core.execution._
import dpsort.core.utils.SortUtils
import dpsort.master.execution.{EmptyStage, GenBlockStage, LocalSortStage, SampleKeyStage, StageExitStatus, TerminateStage}
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MasterContext extends Role with Logging {

  val registryLock = new java.util.concurrent.locks.ReentrantLock

  var lastStageExitStatus = StageExitStatus.SUCCESS
  var recordsCount: Int = 0
  var sampledKeys: mutable.ArrayBuffer[Array[Byte]] = mutable.ArrayBuffer[Array[Byte]]()
  var partitionFunction: mutable.Map[Array[Byte], String] = mutable.Map[Array[Byte], String]() // TODO any better design?

  override def initialize = {
    // Start networking services
    MasterTaskServer.startServer
    HeartBeatServer.startServer
  }

  override def terminate = {
    MasterTaskServer.stopServer
    HeartBeatServer.stopServer
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

    val stageLast = new TerminateStage
    lastStageExitStatus = stageLast.executeAndWaitForTermination()


    // Execute PartitionAndShuffleStage

    // Execute MergeStage (iterate)

    // Execute TerminateStage

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

    val pivots = (0 to wNum-1).map( idx => {    // Key less or equal than pivot will be assigned to that slot
      if ( idx == wNum-1 ) { MAX_KEY }
      else { keysArr( idx * slidingSize ) } }
    )

    // TODO zip with location


  }


}
