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

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import org.apache.logging.log4j.scala.Logging

import dpsort.core.network._
import dpsort.core.network.{Channel, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, ShuffleDataMsg, ShuffleRequestMsg, TaskReportMsg}
import dpsort.worker.WorkerParams


class MasterReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val masterTaskChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val masterTaskBlockingStub = MasterTaskServiceGrpc.blockingStub( masterTaskChannel )

  /* Blocking request */
  def registerWorker(request: RegistryMsg): ResponseMsg = {
    try {
      val response = masterTaskBlockingStub.registerWorker(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"RPC failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

  /* Blocking request */
  def reportTaskResult( request: TaskReportMsg ): ResponseMsg = {
    try {
      val response = masterTaskBlockingStub.reportTaskResult(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Task report failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }
  def shutdown(): Unit = {
    masterTaskChannel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }
}

class ShuffleReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val shuffleChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val shuffleBlockingStub = ShuffleServiceGrpc.blockingStub( shuffleChannel )
  private val shuffleNonBlockingStub = ShuffleServiceGrpc.stub( shuffleChannel )

  /* Blocking request */
  def requestShuffle( request: ShuffleRequestMsg ): ResponseMsg = {
    try {
      logger.debug("requesting shuffle")
      val response = shuffleBlockingStub.requestShuffle(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Shuffle request failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

  /* Non-Blocking request */
  def sendShuffleData( request: ShuffleDataMsg ): Future[ResponseMsg] = {
    try {
      logger.debug("sending shuffle data")
      val response: Future[ResponseMsg] = shuffleNonBlockingStub.sendShuffleData(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Shuffle data transmission failed: ${e.getStatus.toString}")
        throw e
    }
  }

  def shutdown(): Unit = {
    shuffleChannel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

}