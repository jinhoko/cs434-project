package dpsort.core.execution

import scala.collection._
import java.io._
import org.apache.logging.log4j.scala.Logging


object TaskStatus extends Enumeration {
  val WAITING, SUBMITTED, SUCCESS, FAILURE = Value
}

trait Task {
  val id : Int
  val wid : Int
  var status : TaskStatus.Value
  val inputPartition : Unit // TODO define type (there can be both cases of task)
  val outputPartition : Unit

  def getId : Int = id
  def getStatus : TaskStatus.Value = status
  def setStatus(st : TaskStatus.Value): Unit = { status = st }
  def getWorkerID: Int = wid

  def isFinished: Boolean = { status == TaskStatus.SUCCESS || status == TaskStatus.FAILURE }
  def isSubmitted: Boolean = { status != TaskStatus.WAITING }
  def run : Unit
}

/*
 * SerialVersionUID
 * - 1000L : EmptyTask
 * - 1001L : GenBlockTask
 * - 1002L : TerminateTask
 * - 1003L : PartitionAndShuffleTask
 * - 1004L :
 * - 1005L :
 */

abstract class BaseTask( i: Int,
                         wi: Int,
                         st: TaskStatus.Value,
                         inputPart: Unit,
                         outputPart: Unit
                       ) extends Task with Serializable with Logging {
  protected val id: Int = i
  protected val wid: Int = wi
  protected var status: TaskStatus.Value = st
  protected val inputPartition: Unit = inputPart
  protected val outputPartition: Unit = outputPart
}


// Not used in practice. Just for development purpose.
@SerialVersionUID(1000L)
final class EmptyTask( i: Int,
                       wi: Int,
                       st: TaskStatus.Value,
                       inputPart: Unit,
                       outputPart: Unit
                     ) extends BaseTask(i, wi, st, inputPart, outputPart) with Serializable {

  def run = { println("emptytask : wait for 5s and finish"); Thread.sleep(5000) }
}

@SerialVersionUID(1001L)
final class GenBlockTask(  i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: Unit,
                           outputPart: Unit
                   ) extends BaseTask(i, wi, st, inputPart, outputPart) with Serializable {

  def run() = GenBlockContext.run( this )
}

@SerialVersionUID(1002L)
final class TerminateTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: Unit,
                           outputPart: Unit
                   ) extends BaseTask(i, wi, st, inputPart, outputPart) with Serializable {

  def run() = TerminateContext.run( this )
}

@SerialVersionUID(1002L)
final class PartitionAndShuffleTask( i: Int,
                                     wi: Int,
                                     st: TaskStatus.Value,
                                     inputPart: Unit,
                                     outputPart: Unit,
                                     partitionFunc: Unit  // todo type?
                         ) extends BaseTask(i, st, inputPart, outputPart) with Serializable {

  val partitioningFunction = partitionFunc

  def run() = PartitionAndShuffleContext.run( this )
}


// TODO other tasks as well
// serialization : https://alvinalexander.com/scala/how-to-use-serialization-in-scala-serializable-trait/
//