package dpsort.master.execution

import dpsort.core.execution._
import dpsort.core.network.TaskReportMsg
import dpsort.core.network.TaskReportMsg.TaskResultType
import dpsort.core.utils.IdUtils._
import dpsort.core.utils.PartitionUtils._
import dpsort.core.utils.{IdUtils, PartitionUtils}
import dpsort.core.utils.SerializationUtils._
import dpsort.master.{MasterContext, PartitionMetaStore, TaskRunner}
import dpsort.master.MasterConf._
import dpsort.master.TaskRunner._
import org.apache.logging.log4j.scala.Logging

import scala.math
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

object StageExitStatus extends Enumeration {
  val SUCCESS, FAILURE = Value
}

trait Stage extends Logging {

  protected val stageTaskSet: TaskSet = genTaskSet()
  logger.info(s"taskset generated with ${stageTaskSet.getNumTasks} task(s)")

  protected def genTaskSet(): TaskSet

  def taskSet: Set[BaseTask] = stageTaskSet.taskSet

  def terminateCondition: Boolean = stageTaskSet.isAllTasksFinished
  def stageResult: Boolean = stageTaskSet.isAllTasksSucceeded
  def numFinishedTasks: Int = stageTaskSet.getNumFinishedTasks
  def numRemainingTasks: Int = stageTaskSet.getNumRemainingTasks

  def toString: String

  def taskResultHandler( taskRes: TaskReportMsg ): Unit = {
    val taskResultStatus = taskRes.taskResult match {
      case TaskResultType.SUCCESS => TaskStatus.SUCCESS
      case TaskResultType.FAILED  => TaskStatus.FAILURE
    }
    stageTaskSet.getTask( taskRes.taskId ).setStatus( taskResultStatus )
  }

  def executeAndWaitForTermination(): StageExitStatus.Value = {
    logger.info(s"Register stage ${this.toString}.")
    val registerResult = TaskRunner.registerStage(this )
    if( !registerResult ) {
      logger.error(s"Cannot register stage to TaskRunner")
      return StageExitStatus.FAILURE
    }
    logger.info(s"Executing stage ${this.toString}...")

    val stageExitCode: Int = Await.result( executeStage, Duration.Inf )

    logger.info(s"Stage ${this.toString} exited with code ${stageExitCode}")
    val stageExitStatus = stageExitCode match  {
      case 0 => {
        logger.info(s"Stage ${this.toString} finally marked success.")
        StageExitStatus.SUCCESS
      }
      case _ => {
        logger.error(s"Stage ${this.toString} finally marked failure.")
        StageExitStatus.FAILURE
      }
    }
    stageExitStatus
  }
}

class EmptyStage extends Stage {
  override def toString: String = "EmptyStage"
  override protected def genTaskSet(): TaskSet = {
    val taskSeq: Iterable[BaseTask] = {
      for (i <- 0 to 9;
           wid <- PartitionMetaStore.getWorkerIds)
        yield new EmptyTask(genNewTaskID, wid, TaskStatus.WAITING, Unit, Unit)
    }
    logger.info(s"${taskSeq.size} task(s) generated")
    new TaskSet( Random.shuffle( taskSeq ) ) // for fair scheduling
  }
}

class TerminateStage extends Stage {
  override def toString: String = "TerminateStage"
  // TODO need to generate task for all partitions

  override protected def genTaskSet(): TaskSet = {
    val taskSeq: Iterable[BaseTask] = {
      for ( wid <- PartitionMetaStore.getWorkerIds )
        yield new TerminateTask(genNewTaskID, wid, TaskStatus.WAITING, Unit, Unit)
    }
    new TaskSet( Random.shuffle( taskSeq ) ) // for fair scheduling
  }

}

class GenBlockStage extends Stage {
  override def toString: String = "GenBlockStage"

  override protected def genTaskSet(): TaskSet = {
    val taskSeq: Iterable[BaseTask] = {
      for ( wid <- PartitionMetaStore.getWorkerIds )
        yield {
          val parts = PartitionMetaStore.getPartitionList( wid )
          parts.map( pMeta => {
            val offsets: Array[(Int, Int)] = blockOffsetsGenerator( pMeta, get("dpsort.master.blockSizeInLines").toInt )
            val outPNames: Array[String] = {1 to offsets.size}.toArray.map( _ => genNewPartID )
            assert( offsets.size == outPNames.size )
            new GenBlockTask( genNewTaskID, wid, TaskStatus.WAITING, pMeta.pName, outPNames, offsets )
          }).toArray
        }
    }.flatten
    new TaskSet( Random.shuffle( taskSeq ) )
  }

  override def taskResultHandler(taskRes: TaskReportMsg): Unit = {
    if( taskRes.taskResult == TaskResultType.SUCCESS ) {
      val task = stageTaskSet.getTask( taskRes.taskId )
      val wid = task.getWorkerID
      PartitionMetaStore.delPartitionMeta( wid, task.inputPartition )
      task.outputPartition.foreach(
        outPart => {
          PartitionMetaStore.genAndAddPartitionMeta( wid, outPart )
        }
      )
    }
    super.taskResultHandler( taskRes )
  }

}

class LocalSortStage extends Stage {

  override def toString: String = "LocalSortStage"

  override protected def genTaskSet(): TaskSet = {
    val taskSeq: Iterable[BaseTask] = {
    for (wid <- PartitionMetaStore.getWorkerIds)
      yield {
        val parts = PartitionMetaStore.getPartitionList(wid)
        parts.map(pMeta => {
          new LocalSortTask(genNewTaskID, wid, TaskStatus.WAITING, pMeta.pName, genNewPartID )
        }).toArray
      }
    }.flatten
    new TaskSet( Random.shuffle( taskSeq ) )
}

  override def taskResultHandler(taskRes: TaskReportMsg): Unit = {
    if( taskRes.taskResult == TaskResultType.SUCCESS ) {
      val task = stageTaskSet.getTask( taskRes.taskId )
      val wid = task.getWorkerID
      PartitionMetaStore.delPartitionMeta( wid, task.inputPartition )
      task.outputPartition.foreach(
        outPart => PartitionMetaStore.genAndAddPartitionMeta( wid, outPart )
      )
    }
    super.taskResultHandler( taskRes )
  }

}

class SampleKeyStage extends Stage {
  override def toString: String = "SampleKeyStage"

  override protected def genTaskSet(): TaskSet = {
    val recordsCnt = MasterContext.recordsCount
    val sampleRatio = math.min( get("dpsort.master.maxSampleSize").toFloat / recordsCnt.toFloat
                                ,get("dpsort.master.maxSampleRatio").toFloat )
    logger.info(s"sample ratio is set to ${sampleRatio}")
    val taskSeq: Iterable[BaseTask] = {
      for (wid <- PartitionMetaStore.getWorkerIds)
        yield {
          val parts = PartitionMetaStore.getPartitionList(wid)
          parts.map(pMeta => {
            new SampleKeyTask(genNewTaskID, wid, TaskStatus.WAITING, pMeta.pName, genNewPartID, sampleRatio )
          }).toArray
        }
    }.flatten
    new TaskSet( Random.shuffle( taskSeq ) )
  }

  override def taskResultHandler(taskRes: TaskReportMsg): Unit = {
    val sampledKeys = deserializeByteStringToObject( taskRes.serializedTaskResultData )
      .asInstanceOf[Array[Array[Byte]]]
    MasterContext.registryLock.lock()
    MasterContext.sampledKeys ++= sampledKeys
    MasterContext.registryLock.unlock()
    super.taskResultHandler( taskRes )
  }
}

class PartitionAndShuffleStage extends Stage {
  override def toString: String = "PartitionAndShuffleStage"

  override protected def genTaskSet(): TaskSet = {
    val pFunc = MasterContext.partitionFunction
    val taskSeq: Iterable[BaseTask] = {
      for ( wid <- PartitionMetaStore.getWorkerIds )
        yield {
          val parts = PartitionMetaStore.getPartitionList( wid )
          parts.map( pMeta => {
            val outPNames: Array[String] = {1 to PartitionMetaStore.getWorkerIds.size}.toArray.map( _ => genNewPartID() )
            new PartitionAndShuffleTask( genNewTaskID, wid, TaskStatus.WAITING, pMeta.pName, outPNames, pFunc )
          }).toArray
        }
    }.flatten
    new TaskSet( Random.shuffle( taskSeq ) )
  }

  override def taskResultHandler(taskRes: TaskReportMsg): Unit = {

    println(s">> task ${taskRes.taskId} done.")
    // TOdO write

    super.taskResultHandler( taskRes )
  }

}