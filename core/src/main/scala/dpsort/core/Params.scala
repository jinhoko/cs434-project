package dpsort.core

import org.apache.logging.log4j.scala.Logging

trait Params extends Logging {
  // Parameter definition
  // ...

  // NOTE : must be rewrited from the class implementation
  protected def setParams(params: Array[String])

  def applyParams( params: Array[String] ): Unit = {
    try { setParams(params) }
    catch {
      case v : Throwable => {
        logger.error("Wrong parameters are given")
        throw v
      }
    }
  }

}
