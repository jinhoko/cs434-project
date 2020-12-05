package dpsort.master

import dpsort.core.execution.Role
import dpsort.core.execution._
import dpsort.master.execution.{EmptyStage, StageExitStatus}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MasterContext extends Role with Logging {

  var lastStageExitStatus = StageExitStatus.SUCCESS
  lazy val channelMap = Unit // TODO workerMetastore.getallIDs.blabla


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
        Thread.sleep(1000)
      }
    }
    val workerRegistryWait = Await.result(workerRegistryWaitCondition, Duration.Inf )
    logger.info(s"all ${MasterParams.NUM_SLAVES_INT} workers registered")

    // dev : EmptyStage
    val stage0 = new EmptyStage
    lastStageExitStatus = stage0.executeAndWaitForTermination()

    // Execute GenBlockStage
//    val stage1 = new GenBlockStage
//    lastStageExitStatus = stage1.executeAndWaitForTermination()

    // Execute LocalSortStage

    // Execute SampleKeysStage

    // Execute PartitionAndShuffleStage

    // Execute MergeStage (iterate)

    // Execute TerminateStage

  }


}
