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

package dpsort.core.network

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder, ServerServiceDefinition}


trait ServerInterface {
  val server : ServerContext
  def startServer () = { server.start() }
  def stopServer () = { server.stop() }
}

class ServerContext ( service : ServerServiceDefinition,
                      serverName : String,
                      port : Int
                    ) extends Logging {

  self => private[this] var server: Server = null

  def start(): Unit = {
    server = ServerBuilder.forPort( port )
      .addService(service).build.start
    logger.info(s"${serverName} started, listening on " + port)
    sys.addShutdownHook {
      logger.info(s"safe shutdown ${serverName} with JVM termination")
      self.stop()
      logger.info(s"${serverName} shutdown")
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
      logger.info(s"${serverName} shutdown")
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }
}