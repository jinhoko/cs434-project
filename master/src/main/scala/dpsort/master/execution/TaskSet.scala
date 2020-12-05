package dpsort.master.execution

import dpsort.core.execution.{BaseTask, Task}
import org.apache.logging.log4j.scala.Logging

// TaskSet is simply a set of BaseTask objects.
class TaskSet( ts: Iterable[BaseTask] ) extends Logging{

  private val taskSet: Set[BaseTask] = ts.toSet

  // need iterator function from basetask to access


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

}
