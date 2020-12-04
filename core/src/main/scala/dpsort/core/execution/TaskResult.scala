package dpsort.core.execution

import dpsort.core.execution.TaskStatus

@SerialVersionUID(2000L)
final class TaskResult(tid: Int, tr: TaskStatus.Value) extends Serializable {
  val taskID = tid
  val taskStatus = tr
}
