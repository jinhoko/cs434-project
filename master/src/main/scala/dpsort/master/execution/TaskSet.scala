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

package dpsort.master.execution

import org.apache.logging.log4j.scala.Logging

import dpsort.core.execution.{BaseTask, Task, TaskStatus}


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
