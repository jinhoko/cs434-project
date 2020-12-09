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

package dpsort.master

import java.util.concurrent.TimeUnit

import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}

import dpsort.core.network.{Channel, MasterTaskServiceGrpc, RegistryMsg, ResponseMsg, TaskMsg, WorkerTaskServiceGrpc}


class TaskReqChannel( ipPort: (String, Int) ) extends Channel with Logging {

  private val taskReqChannel = ManagedChannelBuilder
    .forAddress( ipPort._1, ipPort._2 )
    .usePlaintext.build

  private val workerTaskBlockingStub = WorkerTaskServiceGrpc.blockingStub( taskReqChannel)

  def requestTask(request: TaskMsg): ResponseMsg = {
    try {
      val response = workerTaskBlockingStub.requestTask(request)
      response
    } catch {
      case e: StatusRuntimeException =>
        logger.error(s"Task Request failed: ${e.getStatus.toString}")
        new ResponseMsg( ResponseMsg.ResponseType.REQUEST_ERROR )
    }
  }

  def shutdown(): Unit = {
    taskReqChannel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

}
