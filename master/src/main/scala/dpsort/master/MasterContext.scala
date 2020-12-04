package dpsort.master

import dpsort.core.execution.Role
import dpsort.master.MasterConf
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MasterContext extends Role with Logging {

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
      while( WorkerMetaStore.getWaitingWorkersNum >0 ) {
        Thread.sleep(1000)
      }
    }
    val workerRegistryWait = Await.result(workerRegistryWaitCondition, Duration.Inf )
    logger.info(s"all ${MasterParams.NUM_SLAVES_INT} workers registered")

    // Execute GenBlockStage

    // Execute LocalSortStage

    // Execute SampleKeysStage

    // Execute PartitionAndShuffleStage

    // Execute MergeStage

    // Execute TerminateStage

  }


}
