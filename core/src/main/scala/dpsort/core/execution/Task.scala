package dpsort.core.execution

import scala.collection._
import java.io._
import org.apache.logging.log4j.scala.Logging


object TaskStatus extends Enumeration {
  val WAITING, SUBMITTED, SUCCESS, FAILURE = Value
}

trait Task extends Logging {
  val id : Int
  var status : TaskStatus.Value
  val inputPartition : Unit // TODO define type (there can be both cases of task)
  val outputPartition : Unit

  def getId : Int = { id }
  def getStatus : TaskStatus.Value = { status }
  def setStatus(st : TaskStatus.Value): Unit = { status = st }
  def run : Unit // TODO define type
}

abstract class BaseTask( i: Int,
                         st: TaskStatus.Value,
                         inputPart: Unit,
                         outputPart: Unit
                       ) extends Task {

  val id: Int = i
  var status: TaskStatus.Value = st
  val inputPartition: Unit = inputPart
  val outputPartition: Unit = outputPart
}

/*
 * SerialVersionUID
 * - 1001L : GenBlockTask
 * - 1002L : TerminateTask
 * - 1003L :
 * - 1004L :
 * - 1005L :
 */
@SerialVersionUID(1001L)
class GenBlockTask(  i: Int,
                     st: TaskStatus.Value,
                     inputPart: Unit,
                     outputPart: Unit
                   ) extends BaseTask(i, st, inputPart, outputPart) with Serializable {

  override def run(): Unit = {

  }

}

@SerialVersionUID(1002L)
class TerminateTask( i: Int,
                     st: TaskStatus.Value,
                     inputPart: Unit,
                     outputPart: Unit
                   ) extends BaseTask(i, st, inputPart, outputPart) with Serializable {

  override def run(): Unit = {

  }

}

// TODO other tasks as well