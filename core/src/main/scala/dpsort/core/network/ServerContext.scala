package dpsort.core.network

import io.grpc.{Server, ServerBuilder, ServerServiceDefinition}
import org.apache.logging.log4j.scala.Logging


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