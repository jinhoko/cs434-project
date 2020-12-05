package dpsort.master.execution

import dpsort.core.execution.{BaseTask, Task, TaskStatus}
import org.apache.logging.log4j.scala.Logging

// TaskSet is simply a set of BaseTask objects.
class TaskSet( ts: Iterable[BaseTask] ) extends Logging{

  val taskSet: Set[BaseTask] = ts.toSet

  def getNumTasks: Int = taskSet.size
  def getTask( tid: Int ): BaseTask = {
    for( t <- taskSet ) {
      if( t.getId == tid ) {
        return t
      }
    }
    throw new NullPointerException()
  }

  def getNumFinishedTasks: Int = taskSet.count( _.isFinished )
  def getNumRemainingTasks: Int = taskSet.count( ! _.isFinished )
  def isAllTasksFinished: Boolean = {
    taskSet.map( _.isFinished ).reduce( (a, b) => a && b )
  }
  def isAllTasksSucceeded: Boolean = {
    taskSet.map( _.getStatus == TaskStatus.SUCCESS ).reduce( (a, b) => a && b )
  }

}
