package dpsort.core.execution

import scala.collection._
import java.io._

import dpsort.core.network.TaskReportMsg.TaskResultType
import org.apache.logging.log4j.scala.Logging

// TODO need code clearing for this file!!

object TaskStatus extends Enumeration {
  val WAITING, SUBMITTED, SUCCESS, FAILURE = Value
}

object TaskType extends Enumeration { // TODO add types
  val EMPTYTASK, TERMINATETASK, GENBLOCKTASK, LOCALSORTTASK, SAMPLEKEYTASK = Value
}

trait Task {
  protected val id : Int
  protected val wid : Int
  protected val taskType: TaskType.Value
  protected var status : TaskStatus.Value
  protected val inputPartition: String
  protected val outputPartition: Array[String]
  protected val offsets: Array[(Int, Int)]
  protected val partitionFunc: Unit

  def getId : Int = id
  def getStatus : TaskStatus.Value = status
  def setStatus(st : TaskStatus.Value): Unit = { status = st }
  def getWorkerID: Int = wid
  def getTaskType: TaskType.Value = taskType

  def isFinished: Boolean = { status == TaskStatus.SUCCESS || status == TaskStatus.FAILURE }
  def isSubmitted: Boolean = { status != TaskStatus.WAITING }
}

/*
 * SerialVersionUID
 * - 1000L : EmptyTask
 * - 1001L : GenBlockTask
 * - 1002L : TerminateTask
 * - 1003L : LocalSortTask
 * - 1004L : PartitionAndShuffleTask
 * - 1005L : SampleKeyTask
 */

abstract class BaseTask( i: Int,
                         wi: Int,
                         tty: TaskType.Value,
                         st: TaskStatus.Value,
                         inputPart: String,
                         outputPart: Array[String],
                         off: Array[(Int, Int)],
                         pFunc: Unit,
                         sr: Float
                       ) extends Task with Serializable with Logging {
  protected val id: Int = i
  protected val taskType: TaskType.Value = tty
  protected val wid: Int = wi
  protected var status: TaskStatus.Value = st
  val inputPartition: String = inputPart
  val outputPartition: Array[String] = outputPart
  val offsets: Array[(Int, Int)] = off
  val partitionFunc: Unit = pFunc
  val sampleRatio: Float = sr
  // TODO add terminateStatus
}


// Not used in practice. Just for development purpose.
@SerialVersionUID(1000L)
final class EmptyTask( i: Int,
                       wi: Int,
                       st: TaskStatus.Value,
                       inputPart: Unit,
                       outputPart: Unit
                     ) extends BaseTask(i, wi, TaskType.EMPTYTASK, st, null, null, null, null, 0) with Serializable {
}

@SerialVersionUID(1001L)
final class GenBlockTask(  i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: Array[String],
                           offsets: Array[(Int, Int)]
                   ) extends BaseTask(i, wi, TaskType.GENBLOCKTASK, st, inputPart, outputPart, offsets, null, 0) with Serializable {

}

@SerialVersionUID(1002L)
final class TerminateTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: Unit,
                           outputPart: Unit,
                   ) extends BaseTask(i, wi, TaskType.TERMINATETASK, st, null, null, null, null, 0) with Serializable {
}

@SerialVersionUID(1003L)
final class LocalSortTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: String,
                         ) extends BaseTask(i, wi, TaskType.LOCALSORTTASK, st, inputPart, Array[String](outputPart), null, null, 0) with Serializable {
}

@SerialVersionUID(1005L)
final class SampleKeyTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: String,
                           sampleRatio: Float,
                         ) extends BaseTask(i, wi, TaskType.SAMPLEKEYTASK, st, inputPart, Array[String](outputPart), null, null, sampleRatio) with Serializable {
}


//@SerialVersionUID(1004L)
//final class PartitionAndShuffleTask( i: Int,
//                                     wi: Int,
//                                     st: TaskStatus.Value,
//                                     inputPart: Unit,
//                                     outputPart: Unit,
//                                     partitionFunc: Unit  // todo type?
//                         ) extends BaseTask(i, wi, st, inputPart, outputPart) with Serializable {
//
//  val partitioningFunction = partitionFunc
//
//  def run() = PartitionAndShuffleContext.run( this )
//}


// TODO other tasks as well
// serialization : https://alvinalexander.com/scala/how-to-use-serialization-in-scala-serializable-trait/
//