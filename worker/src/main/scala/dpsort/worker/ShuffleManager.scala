package dpsort.worker

import java.util.concurrent.locks.ReentrantLock

import dpsort.core.execution.BaseTask
import dpsort.core.{MutableRecordLines, PartFunc}
import dpsort.core.network.{ChannelMap, ResponseMsg, ShuffleDataMsg, ShuffleRequestMsg}
import dpsort.core.network.ResponseMsg.ResponseType
import dpsort.worker.WorkerConf.get
import org.apache.logging.log4j.scala.Logging
import dpsort.worker.ShuffleReqChannel
import dpsort.worker.WorkerShuffleServer._
import dpsort.core.utils.SerializationUtils._
import scala.concurrent.ExecutionContext.Implicits.global


import scala.concurrent.Future

object ShuffleStatus extends Enumeration {
  val WAITING, SENT = Value
}

/* Handles both shuffle output and input */
object ShuffleManager extends Logging {

  private val maxShuffleConnections = get("dpsort.worker.maxShuffleConnections").toInt
  private val maxShuffleSendConnections = (maxShuffleConnections/2).toInt
  private val maxShuffleReceiveConnections = (maxShuffleConnections/2).toInt

  var numOngoingSendShuffles = 0
  private def numShuffleSendConnectionsLeft = maxShuffleSendConnections - numOngoingSendShuffles
  val shuffleSendLock: ReentrantLock = new ReentrantLock()  // Lock that has information of acquiring order
  private def isShuffleSendAvailable: Boolean = numShuffleSendConnectionsLeft > 0

  var numOngoingReceiveShuffles = 0
  private def numShuffleReceiveConnectionsLeft = maxShuffleReceiveConnections - numOngoingReceiveShuffles
  val shuffleReceiveLock: ReentrantLock = new ReentrantLock()  // Lock that has information of acquiring order
  private def isShuffleReceiveAvailable: Boolean = numShuffleReceiveConnectionsLeft > 0

  /* Shuffle In */
  def shuffleRequestHandler: ResponseMsg = {
    var response: ResponseMsg = null
    shuffleReceiveLock.lock()
    if( isShuffleReceiveAvailable ) {
      logger.debug(s"accepting shuffle request")
      numOngoingReceiveShuffles += 1
      response = new ResponseMsg( ResponseType.NORMAL )
    } else {
      logger.debug(s"shuffle request declined")
      response = new ResponseMsg( ResponseType.HANDLE_ERROR )
    }
    shuffleReceiveLock.unlock()
    response
  }

  /* Shuffle Out */
  def shuffleOut( task: BaseTask, partFunc: PartFunc,  partitions: Array[MutableRecordLines], localPartIdx:Int ) = {

    // generate status list (except mine)
    val statusArr: Array[ShuffleStatus.Value] = Array.fill[ShuffleStatus.Value]( partFunc.size )( ShuffleStatus.WAITING )
    var numFinishedShuffle = 0
    def isShuffleNotSubmitted( v: ShuffleStatus.Value ) = v == ShuffleStatus.WAITING

    def shuffleFinishCondition =
      numFinishedShuffle == statusArr.size

    // Mark locally saved file as finished
    logger.debug("shuffle local write done")
    statusArr(localPartIdx) = ShuffleStatus.SENT
    numFinishedShuffle += 1

    while( ! shuffleFinishCondition ) {
      for( (stat, idx) <- statusArr.zipWithIndex ) {
        if( isShuffleNotSubmitted( stat ) ) {
          val targetAddr:(String,Int) = partFunc(idx)._2
          val channel:ShuffleReqChannel =
            ChannelMap.getOrAddChannel( targetAddr, new ShuffleReqChannel( targetAddr ) ).asInstanceOf[ShuffleReqChannel]
          var shuffleGranted = false

          shuffleSendLock.lock()
          try{
            if( isShuffleSendAvailable ) {
              val response: ResponseMsg = channel.requestShuffle( new ShuffleRequestMsg() )
              if( response.response == ResponseType.NORMAL ) {
                shuffleGranted = true
                numOngoingSendShuffles += 1
              }
            }
          } finally {
            shuffleSendLock.unlock()
          }

          // After approval message, each task is guaranteed
          // to immediately transmit the data

          if( shuffleGranted ) {
            logger.debug(s"shuffle request granted : now transmitting partition ${task.outputPartition(idx)} to " +
              s"${targetAddr._1}:${targetAddr._2.toString}")
            statusArr(idx) = ShuffleStatus.SENT
            val serializedPName = serializeObjectToByteString( task.outputPartition(idx) )
            val serializedPData = serializeObjectToByteString( partitions(idx).toArray: Array[Array[Byte]] )
            val requestMsg = new ShuffleDataMsg( serializedPName, serializedPData )

            val response: Future[ResponseMsg] = channel.sendShuffleData( requestMsg )
            response onComplete {
              case _ => {
                shuffleSendLock.lock()
                numOngoingSendShuffles -= 1
                shuffleSendLock.unlock()
                numFinishedShuffle += 1
              }
            }
          }

        }
      }
      Thread.sleep(3000)
      logger.info(s"shufflemanager status report : ${statusArr.count( v => ! isShuffleNotSubmitted(v))}" +
        s" shuffle(s) submitted / finished. ")
    }
  }

}
