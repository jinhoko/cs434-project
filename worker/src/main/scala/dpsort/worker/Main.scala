package dpsort.worker

import dpsort.worker.WorkerContext

import org.apache.logging.log4j.scala.Logging


object Main extends Logging {
  def main(args : Array[String]): Unit = {
    // Load and instantiate WorkerConf
    logger.info("load configurations")
    WorkerConf
    // Parser argument and Register to object
    logger.info("load arguments")
    WorkerParams.applyParams(args)

    // run workercontext
    logger.info("dpsort worker starting")
    WorkerContext.initialize
    WorkerContext.execute
    WorkerContext.terminate
    logger.info("dpsort worker finished")
  }
}
