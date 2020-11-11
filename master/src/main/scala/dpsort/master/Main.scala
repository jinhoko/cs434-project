package dpsort.master

import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level


object Main extends Logging {
  def main(args: Array[String]): Unit = {
    def a (i : Int): Unit = {
      if (i==0) return
      logger.info("testlog!!")
      a(i-1)
    }
    logger.info("testlog!!")
    logger.info("testlog!!")
    logger.info("testlog!!")
    logger.fatal("log4j:logger.fatal()")
    logger.fatal("log4j:logger.fatal()")

    a (1000000000)
  }
}
