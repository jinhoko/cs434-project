package dpsort.worker.execution

import com.google.protobuf.ByteString
import com.sun.org.apache.xml.internal.utils.ThreadControllerWrapper
import dpsort.core.execution._
import dpsort.core.execution.TaskType
import dpsort.core.utils.FileUtils._
import dpsort.worker.wUtils.PartitionUtils._
import dpsort.worker.WorkerConf._
import org.apache.logging.log4j.scala.Logging
import dpsort.core.utils.SortUtils
import dpsort.core.utils.SerializationUtils.serializeObjectToByteString
import dpsort.core.{KEY_OFFSET_BYTES, LINE_SIZE_BYTES, MutableRecordLines, PartFunc, RecordLines}
import dpsort.worker.ShuffleManager

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ExecCtxtFetcher {
  def getContext( task: BaseTask ): TaskExecutionContext = {
    val tType = task.getTaskType
    tType match {
      case TaskType.EMPTYTASK => EmptyContext
      case TaskType.GENBLOCKTASK => GenBlockContext
      case TaskType.TERMINATETASK => TerminateContext
      case TaskType.LOCALSORTTASK => LocalSortContext
      case TaskType.SAMPLEKEYTASK => SampleKeyContext
      case TaskType.PARTITIONANDSHUFFLETASK => PartitionAndShuffleContext
      // TODO write more
    }
  }
}


trait TaskExecutionContext {
  def run( _task: BaseTask ): Either[Unit, ByteString]
}

object EmptyContext extends TaskExecutionContext {

  def run( _task: BaseTask ) = {
    val task = _task.asInstanceOf[EmptyTask]
    val rndTime = new scala.util.Random(task.getId).nextInt(10)
    println(s"this is emptytask : wait for ${rndTime}(s) and finish");
    Thread.sleep( rndTime * 1000 )

    Left( Unit )
  }

}

object GenBlockContext extends TaskExecutionContext with Logging {

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
      Left( Unit )
    } catch {
      case e: Throwable => {
        logger.error("failed to write partition")
        throw e
      }
    }
  }

}

object LocalSortContext extends TaskExecutionContext with Logging {

  def run(_task: BaseTask) = {
    val task = _task.asInstanceOf[LocalSortTask]
    try {
      val filepath = getPartitionPath( task.inputPartition )
      val outPartName = task.outputPartition.head
      val partLines: RecordLines = fetchLinesFromFile( filepath, LINE_SIZE_BYTES )
      SortUtils.sortLines(partLines)
      writeLinesToFile( partLines, getPartitionPath(outPartName) )
      deleteFile( filepath )
      Left( Unit )
    } catch {
      case e: Throwable => {
        logger.error("failed to write partition")
        throw e
      }
    }
  }

}

object SampleKeyContext extends TaskExecutionContext with Logging {

  override def run(_task: BaseTask) = {
    val task = _task.asInstanceOf[SampleKeyTask]
    try {
      val filepath = getPartitionPath( task.inputPartition )
      val partLines: RecordLines = fetchLinesFromFile( filepath, LINE_SIZE_BYTES )
      val sampledKeys = SortUtils.sampleKeys( partLines, task.sampleRatio, KEY_OFFSET_BYTES )
      val returnObj = serializeObjectToByteString( sampledKeys )
      Right( returnObj )
    } catch {
      case e: Throwable => {
        logger.error("failed to sample partition")
        throw e
      }
    }
  }

}

object PartitionAndShuffleContext extends TaskExecutionContext with Logging {

  def run( _task: BaseTask ) = {
    val task = _task.asInstanceOf[PartitionAndShuffleTask]

    val filepath = getPartitionPath( task.inputPartition )
    val partFunc: PartFunc = task.partitionFunc
    val partitions = Array.fill[MutableRecordLines]( partFunc.size )( new ArrayBuffer[Array[Byte]]() )

    val nLines: Int = getNumLinesInFile( filepath )
    SortUtils.splitPartitions( filepath, partFunc, partitions, nLines,  LINE_SIZE_BYTES )

    logger.debug(s"partitioning done")
    partitions.zipWithIndex.foreach( bkidx => { logger.debug(s"${bkidx._2.toString} : ${bkidx._1.size}") } )

    val partToStoreIdx = partFunc.zipWithIndex
      .filter( pi => ( pi._1._2._1 equals get("dpsort.worker.ip") )
                    && pi._1._2._2 == get("dpsort.worker.shufflePort").toInt )
      .head
      ._2

    // write locally first
    val linesToStore: Array[Array[Byte]] = partitions(partToStoreIdx).toArray
    writeLinesToFile( linesToStore, getPartitionPath( task.outputPartition(partToStoreIdx) ) )

    // shuffle out the rest
    ShuffleManager.shuffleOut( task, partFunc, partitions, partToStoreIdx)

    // TODO delete original partition


    Left( Unit )
  }

}


object TerminateContext extends TaskExecutionContext  {
  def run( _task: BaseTask ) = {
    val task = _task.asInstanceOf[TerminateTask]

    // TODO need to writeback to PMS
    // TODO terminate task는 딱 한번만 실행되어야 함.
    Left( Unit )
  }
}


// TODO