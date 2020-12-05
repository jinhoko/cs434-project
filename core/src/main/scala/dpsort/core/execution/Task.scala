package dpsort.core.execution

import scala.collection._
import java.io._

import dpsort.core.network.TaskReportMsg.TaskResultType
import org.apache.logging.log4j.scala.Logging


object TaskStatus extends Enumeration {
  val WAITING, SUBMITTED, SUCCESS, FAILURE = Value
}

object TaskType extends Enumeration { // TODO add types
  val EMPTYTASK, TERMINATETASK = Value
}

trait Task {
  protected val id : Int
  protected val wid : Int
  protected val taskType: TaskType.Value
  protected var status : TaskStatus.Value
  protected val inputPartition : Unit // TODO define type (there can be both cases of task)
  protected val outputPartition : Unit

  def getId : Int = id
  def getStatus : TaskStatus.Value = status
  def setStatus(st : TaskStatus.Value): Unit = { status = st }
  def getWorkerID: Int = wid
  def getTaskType: TaskType.Value = taskType

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
                         tty: TaskType.Value,
                         st: TaskStatus.Value,
                         inputPart: Unit,
                         outputPart: Unit
                       ) extends Task with Serializable with Logging {
  protected val id: Int = i
  protected val taskType: TaskType.Value = tty
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
                     ) extends BaseTask(i, wi, TaskType.EMPTYTASK, st, inputPart, outputPart) with Serializable {

  def run = {
    val rndTime = new scala.util.Random(this.id).nextInt(10)
    println(s"this is emptytask : wait for ${rndTime}(s) and finish");
    Thread.sleep( rndTime * 1000 )
  }
}

//@SerialVersionUID(1001L)
//final class GenBlockTask(  i: Int,
//                           wi: Int,
//                           st: TaskStatus.Value,
//                           inputPart: Unit,
//                           outputPart: Unit
//                   ) extends BaseTask(i, wi, st, inputPart, outputPart) with Serializable {
//
//  def run() = GenBlockContext.run( this )
//}
//
//@SerialVersionUID(1002L)
//final class TerminateTask( i: Int,
//                           wi: Int,
//                           st: TaskStatus.Value,
//                           inputPart: Unit,
//                           outputPart: Unit
//                   ) extends BaseTask(i, wi, st, inputPart, outputPart) with Serializable {
//
//  def run() = TerminateContext.run( this )
//}
//
//@SerialVersionUID(1002L)
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