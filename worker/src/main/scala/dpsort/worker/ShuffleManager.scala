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

package dpsort.worker

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.locks.ReentrantLock

import org.apache.logging.log4j.scala.Logging

import dpsort.core.execution.BaseTask
import dpsort.core.{MutableRecordLines, PartFunc}
import dpsort.core.network.{ChannelMap, ResponseMsg, ShuffleDataMsg, ShuffleRequestMsg}
import dpsort.core.network.ResponseMsg.ResponseType
import dpsort.core.utils.SerializationUtils._
import dpsort.worker.WorkerConf.get
import dpsort.worker.ShuffleReqChannel
import dpsort.worker.WorkerShuffleServer._


import scala.concurrent.Future

object ShuffleStatus extends Enumeration {
  val WAITING, SENT = Value
}

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

    def shuffleFinishCondition = numFinishedShuffle == statusArr.size
    // Mark locally saved file as finished
    logger.debug("shuffle local write done")
    statusArr(localPartIdx) = ShuffleStatus.SENT
    numFinishedShuffle += 1

    while( ! shuffleFinishCondition ) {
      for( (stat, idx) <- statusArr.zipWithIndex ) {
        if( isShuffleNotSubmitted( stat ) ) {
          val targetAddr:(String,Int) = partFunc(idx)._2
          val channel:ShuffleReqChannel =
            try {
              ChannelMap.getChannel( targetAddr ).asInstanceOf[ShuffleReqChannel]
            } catch {
              case e: Throwable => {
                ChannelMap.addChannel( targetAddr, new ShuffleReqChannel(targetAddr) )
                ChannelMap.getChannel( targetAddr ).asInstanceOf[ShuffleReqChannel]
              }
            }
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
            for( partSegment <- partitions(idx).grouped( get("dpsort.worker.maxShuffleMsgLines").toInt ) ) {
              val serializedPDataSegment = serializeObjectToByteString( partSegment.toArray: Array[Array[Byte]] )
              val requestMsg = new ShuffleDataMsg( serializedPName, serializedPDataSegment )
              val response = channel.sendShuffleData( requestMsg )
            }

            val response = channel.terminateShuffle( new ShuffleRequestMsg() )
            shuffleSendLock.lock()
            numOngoingSendShuffles -= 1
            shuffleSendLock.unlock()
            numFinishedShuffle += 1
            }
          }
        }
      }
      Thread.sleep(3000)
      logger.info(s"shufflemanager status report : ${statusArr.count( v => ! isShuffleNotSubmitted(v))}" +
        s" shuffle(s) submitted / finished. ")
  }

}
