package dpsort.master.execution

import dpsort.core.execution.{BaseTask, Task, TaskStatus}
import org.apache.logging.log4j.scala.Logging

// TaskSet is simply a set of BaseTask objects.
class TaskSet( ts: Iterable[BaseTask] ) extends Logging{

  private val taskSet: Set[BaseTask] = ts.toSet

  def getIterator: Iterator[BaseTask] = taskSet.toIterator

  def getTask( tid: Int ): BaseTask = {
    val foundTask: Option[BaseTask] = taskSet.find( _.id == tid )
    foundTask match {
      case t:BaseTask => t
      case _ => {
        logger.error("task not found")
        throw new NullPointerException()
      }
    }
  }

  def isAllTasksFinished: Boolean = {
    taskSet.map( _.isFinished ).reduce( (a, b) => a && b )
  }
  def isAllTasksSucceeded: Boolean = {
    taskSet.map( _.getStatus == TaskStatus.SUCCESS ).reduce( (a, b) => a && b )
  }

}
