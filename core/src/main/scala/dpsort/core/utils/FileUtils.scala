package dpsort.core.utils

import scala.io.Source
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

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
    val source = Source.fromFile(filePath)
    try {
      source.getLines.size
    } finally {
      source.close()
    }
  }

  def fetchLinesToArray( filePath: String, inputArr: Array[String], stIdx:Int, size:Int  ) = {
    val source = Source.fromFile(filePath)
    try {
      val iter = source.getLines.drop( stIdx )
      iter.copyToArray( inputArr, 0, size )
    } finally {
      source.close()
    }
  }

  def writeLineArrToFile( data: Array[String], path: String ) = {
    val file = new File( path )
    val writer = new PrintWriter( file )
    for ( (line,idx) <- data.zipWithIndex ) {
      writer.write(line)
      writer.write("\n")
    }
    writer.close()
  }
}