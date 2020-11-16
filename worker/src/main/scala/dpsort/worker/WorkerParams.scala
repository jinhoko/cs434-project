package dpsort.worker

import dpsort.core.Params
import dpsort.core.utils.FileUtils.getFilesInDirectory
import org.apache.logging.log4j.scala.Logging

object WorkerParams extends Params with Logging {
  // Parameter definition
  var MASTER_IP_STR: String = ""
  var MASTER_PORT_INT: Int = 0
  var INPUT_FILES_STRARR: Array[String] = Array("")
  var OUTPUT_DIR_STR: String = ""


  // Set parameters. OK to throw exceptions
  protected override def setParams ( params: Array[String] ): Unit = {
    println(params.mkString(" "))
    MASTER_IP_STR = params(0)
    MASTER_PORT_INT = params(1).toInt
    OUTPUT_DIR_STR = params(2)
    INPUT_FILES_STRARR = params.drop(3)
      .flatMap( getFilesInDirectory ).distinct
  }

}