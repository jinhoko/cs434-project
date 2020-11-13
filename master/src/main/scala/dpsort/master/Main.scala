package dpsort.master

import org.apache.logging.log4j.scala.Logging

import dpsort.master.{MasterTaskServer, HeartBeatServer}


object Main extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("dpsort master starting")
    // Load Conf object

    // Start service
    MasterTaskServer.startServer()
    logger.debug("proceed?")

    HeartBeatServer.startServer()
    // Start heartbeat service

    // TODO execute master context

    // Stop services
    MasterTaskServer.stopServer()
    HeartBeatServer.stopServer()
    logger.info("dpsort master finished")
  }
}
