package dpsort.worker.execution

import dpsort.core.execution._
import dpsort.core.execution.TaskType
import dpsort.core.utils.FileUtils._
import dpsort.worker.wUtils.PartitionUtils._
import dpsort.worker.WorkerConf._
import org.apache.logging.log4j.scala.Logging
import dpsort.core.utils.SortUtils
import dpsort.worker.{RecordLines, LINE_SIZE_BYTES}

import scala.io.Source

object ExecCtxtFetcher {
  def getContext( task: BaseTask ): ExecutionContext = {
    val tType = task.getTaskType
    tType match {
      case TaskType.EMPTYTASK => EmptyContext
      case TaskType.GENBLOCKTASK => GenBlockContext
      case TaskType.TERMINATETASK => TerminateContext
      case TaskType.LOCALSORTTASK => LocalSortContext
      // TODO write more
    }
  }
}


trait ExecutionContext {
  def run( _task: BaseTask ): Unit
}

object EmptyContext extends ExecutionContext {

  def run( _task: BaseTask ): Unit = {
    val task = _task.asInstanceOf[EmptyTask]
    val rndTime = new scala.util.Random(task.getId).nextInt(10)
    println(s"this is emptytask : wait for ${rndTime}(s) and finish");
    Thread.sleep( rndTime * 1000 )
  }

}

object GenBlockContext extends ExecutionContext with Logging {

  def run( _task: BaseTask ) = {
    val task = _task.asInstanceOf[GenBlockTask]
    try {
      val filepath = task.inputPartition
      for( (outPartName,pIdx) <- task.outputPartition.zipWithIndex ){
        val stIdx = task.offsets(pIdx)._1 - 1
        val copyLen = task.offsets(pIdx)._2 - task.offsets(pIdx)._1 + 1

        val partLinesArr: RecordLines = fetchLinesFromFile( filepath, stIdx, copyLen, LINE_SIZE_BYTES )
        writeLinesToFile( partLinesArr, getPartitionPath(outPartName) )
      }
    } catch {
      case e: Throwable => {
        logger.error("failed to write partition")
        throw e
      }
    }
  }

}

object LocalSortContext extends ExecutionContext with Logging {

  def run(_task: BaseTask) = {
    val task = _task.asInstanceOf[LocalSortTask]
    try {
      val filepath = getPartitionPath( task.inputPartition )
      val outPartName = task.outputPartition.head
      val partLines: RecordLines = fetchLinesFromFile( filepath, LINE_SIZE_BYTES )
      SortUtils.sortLines(partLines)
      writeLinesToFile( partLines, getPartitionPath(outPartName) )
      deleteFile( filepath )
    } catch {
      case e: Throwable => {
        logger.error("failed to write partition")
        throw e
      }
    }
  }

}

//object PartitionAndShuffleContext {
//  def run( task: PartitionAndShuffleTask ) = {
//
//  }
//}
//

object TerminateContext extends ExecutionContext  {
  def run( _task: BaseTask ) = {
    val task = _task.asInstanceOf[TerminateTask]

    // TODO need to writeback to PMS
    // TODO terminate task는 딱 한번만 실행되어야 함.

  }
}


// TODO