package dpsort.master

import dpsort.core.Params

import org.apache.logging.log4j.scala.Logging

object MasterParams extends Params with Logging {
  // Parameter definition
  var NUM_SLAVES_INT: Int = 0

  // Set parameters. OK to throw exceptions
  protected override def setParams ( params: Array[String] ): Unit = {
    NUM_SLAVES_INT = params(0).toInt
  }

}