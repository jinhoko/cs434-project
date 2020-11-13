package dpsort.master

import org.apache.logging.log4j.scala.Logging

import dpsort.master.MasterConf

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("dpsort master starting")
    // Load Conf object

    // Start service
    MasterTaskServer.startServer()
    HeartBeatServer.startServer()

    // TODO execute master context

    // Stop services
    MasterTaskServer.stopServer()
    HeartBeatServer.stopServer()
    logger.info("dpsort master finished")
  }
}
