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

import org.apache.logging.log4j.scala.Logging

import dpsort.core.Registry
import dpsort.core.execution.Role
import dpsort.core.network.ResponseMsg.ResponseType
import dpsort.core.network.{ChannelMap, RegistryMsg, ResponseMsg}
import dpsort.core.utils.SerializationUtils._
import dpsort.worker.WorkerConf._
import dpsort.core.storage.PartitionMeta
import dpsort.core.utils.FileUtils
import dpsort.worker.MasterReqChannel


object WorkerContext extends Role with Logging {

  override def initialize: Unit = {
    // Start networking services
    WorkerTaskServer.startServer
    WorkerShuffleServer.startServer
    // Open worker channels
    ChannelMap.addChannel( WorkerParams.MASTER_IP_PORT , new MasterReqChannel( WorkerParams.MASTER_IP_PORT ) )
    // Set working directory
    initWorkDir
    // Listing files to partition
    logger.info("following files will be sorted : ")
    WorkerParams.INPUT_FILES_STRARR.foreach( st => logger.info(s"> ${st}") )
  }

  override def terminate: Unit = {
    // Stop task server
    WorkerTaskServer.stopServer
  }

  override def execute: Unit = {
    // Register worker to master
    val reqChannel: MasterReqChannel = ChannelMap.getChannel( WorkerParams.MASTER_IP_PORT )
      .asInstanceOf[MasterReqChannel]
    logger.info(s"trying to register worker via channel : ${reqChannel}")
    val registryResponse: ResponseMsg = reqChannel.registerWorker( genRegistry )
    if( registryResponse.response != ResponseType.NORMAL ) {
      logger.error("registration failure")
      return
    }
    logger.info("registration done")

    // Start TaskManager
    TaskManager.taskManagerContext()
  }

  private def genRegistry() = {
    logger.info("reading inputs to calculate the size.. it may take a while")
    val registryObj = new Registry(
      WorkerConf.get("dpsort.worker.ip"),
      WorkerConf.get("dpsort.worker.port").toInt,
      WorkerConf.get("dpsort.worker.shufflePort").toInt,
      WorkerParams.INPUT_FILES_STRARR.map( new PartitionMeta( _ ) )
    )
    logger.info("read all the inputs. now generating registry message.")
    new RegistryMsg( serializeObjectToByteString( registryObj ) )
  }

  private def initWorkDir(): Unit = {
    FileUtils.makeDirectory( "tmp" )
    FileUtils.createAndClearDirectory( get("dpsort.worker.workdir") )
  }
}
