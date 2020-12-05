package dpsort.master

import org.apache.logging.log4j.scala.Logging


object Main extends Logging {
  def main(args: Array[String]): Unit = {
    // Load and instantiate MasterConf
    logger.info("load configurations")
    MasterConf
    // Parser argument and Register to object
    logger.info("load arguments")
    MasterParams.applyParams(args)

    // run MasterContext
    logger.info("dpsort master starting")
    MasterContext.initialize
    MasterContext.execute
    MasterContext.terminate
    logger.info("dpsort master finished")
  }
}
