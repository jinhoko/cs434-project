package dpsort.master

import org.apache.logging.log4j.scala.Logging


object Main extends Logging {
  def main(args: Array[String]): Unit = {
    def a (): Unit = {
      logger.info("testlog!!")
    }
    printf("hehe")
    a
  }
}
