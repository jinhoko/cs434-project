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
    new File(anyPath).getAbsolutePath
  }

  def checkIfFileExists( filePath: String ): Boolean = {
    val file = new File(filePath)
    file.exists && file.isFile
  }

  def makeDirectory( dirPath:String ) = {
    val a = new File(dirPath).mkdir()
  }

  def createAndClearDirectory( dirPath: String ): Unit = {
    val dir = new File(dirPath)
    if( ! dir.exists() ) {
      dir.mkdir()
    }
    dir.listFiles.foreach( _.delete() )
  }

  def getFilesInDirectory( dirPath: String ): Seq[String] = {
    val dir = new File(dirPath)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).map(_.getCanonicalPath).toSeq
    } else {
      Seq[String]()
    }
  }

  def getNumLinesInFile( filePath: String ): Int = {
    Source.fromFile(filePath).getLines.size
  }

}

// NOTE : function getFilesInDirectory from :
//        http://alvinalexander.com/scala/how-to-list-files-in-directory-filter-names-scala/
