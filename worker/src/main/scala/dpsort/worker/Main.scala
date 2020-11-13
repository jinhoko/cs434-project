package dpsort.worker

import org.apache.logging.log4j.scala.Logging


object Main extends Logging {
  def main(args : Array[String]): Unit = {
    logger.info("dpsort worker starting")
    // TODO execute worker context
    logger.info("dpsort worker finished")
  }
}
