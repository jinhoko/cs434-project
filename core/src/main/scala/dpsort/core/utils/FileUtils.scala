package dpsort.core.utils

import scala.io.Source
import java.io.File

import org.apache.logging.log4j.scala.Logging

/**
 * System PWD is set to $DPSORT_HOME
 * TODO write more
 */
object FileUtils extends Logging {
  private val env = System.getenv("DPSORT_HOME")
  assert( !(env equals "") )
  assert( env equals new File(".").getCanonicalPath )
  logger.info(s"system path : ${env}")

  def getAbsPath( anyPath: String ): String = {
    new File( anyPath ).getAbsolutePath
  }
  def checkIfFileExists( filePath: String ): Boolean = {
    true
  }
  def getFilesInDirectory( path: String ): Seq[String] = {
    // TODO
  }

}