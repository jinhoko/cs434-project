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

package dpsort.core.execution

import java.io._

import org.apache.logging.log4j.scala.Logging

import dpsort.core.PartFunc
import dpsort.core.network.TaskReportMsg.TaskResultType


object TaskStatus extends Enumeration {
  val WAITING, SUBMITTED, SUCCESS, FAILURE = Value
}

object TaskType extends Enumeration {
  val EMPTYTASK,
  GENBLOCKTASK,
  LOCALSORTTASK,
  SAMPLEKEYTASK,
  PARTITIONANDSHUFFLETASK,
  MERGETASK,
  TERMINATETASK = Value
}

trait Task {
  protected val id : Int
  protected val wid : Int
  protected val taskType: TaskType.Value
  protected var status : TaskStatus.Value
  protected val inputPartition: Array[String]
  protected val outputPartition: Array[String]
  protected val offsets: Array[(Int, Int)]
  protected val partitionFunc: PartFunc

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
 * - 1006L : MergeTask
 */

abstract class BaseTask( i: Int,
                         wi: Int,
                         tty: TaskType.Value,
                         st: TaskStatus.Value,
                         inputPart: Array[String],
                         outputPart: Array[String],
                         off: Array[(Int, Int)],
                         pFunc: PartFunc,
                         sr: Float
                       ) extends Task with Serializable with Logging {
  protected val id: Int = i
  protected val taskType: TaskType.Value = tty
  protected val wid: Int = wi
  protected var status: TaskStatus.Value = st
  val inputPartition: Array[String] = inputPart
  val outputPartition: Array[String] = outputPart
  val offsets: Array[(Int, Int)] = off
  val partitionFunc: PartFunc = pFunc
  val sampleRatio: Float = sr
}


// Not used in practice. Just for development purpose.
@SerialVersionUID(1000L)
final class EmptyTask( i: Int,
                       wi: Int,
                       st: TaskStatus.Value,
                       inputPart: Unit,
                       outputPart: Unit
                     )
  extends BaseTask(i, wi, TaskType.EMPTYTASK, st, null, null, null, null, 0)
    with Serializable {
}

@SerialVersionUID(1001L)
final class GenBlockTask(  i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: Array[String],
                           offsets: Array[(Int, Int)]
                   )
  extends BaseTask(i, wi, TaskType.GENBLOCKTASK, st, Array[String](inputPart), outputPart, offsets, null, 0)
    with Serializable {

}

@SerialVersionUID(1002L)
final class TerminateTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: String,
                   )
  extends BaseTask(i, wi, TaskType.TERMINATETASK, st, Array[String](inputPart), Array[String](outputPart), null, null, 0)
    with Serializable {
}

@SerialVersionUID(1003L)
final class LocalSortTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: String,
                         )
  extends BaseTask(i, wi, TaskType.LOCALSORTTASK, st, Array[String](inputPart), Array[String](outputPart), null, null, 0)
    with Serializable {
}

@SerialVersionUID(1005L)
final class SampleKeyTask( i: Int,
                           wi: Int,
                           st: TaskStatus.Value,
                           inputPart: String,
                           outputPart: String,
                           sampleRatio: Float,
                         )
  extends BaseTask(i, wi, TaskType.SAMPLEKEYTASK, st, Array[String](inputPart), Array[String](outputPart), null, null, sampleRatio)
    with Serializable {
}

@SerialVersionUID(1004L)
final class PartitionAndShuffleTask( i: Int,
                                     wi: Int,
                                     st: TaskStatus.Value,
                                     inputPart: String,
                                     outputPart: Array[String],
                                     partitionFunc: PartFunc
                         )
  extends BaseTask(i, wi, TaskType.PARTITIONANDSHUFFLETASK, st, Array[String](inputPart), outputPart, null, partitionFunc, 0)
    with Serializable {
}

@SerialVersionUID(1006L)
final class MergeTask( i: Int,
                       wi: Int,
                       st: TaskStatus.Value,
                       inputPart: Array[String],
                       outputPart: String,
                     )
  extends BaseTask(i, wi, TaskType.MERGETASK, st, inputPart, Array[String](outputPart), null, null, 0)
    with Serializable {
}
