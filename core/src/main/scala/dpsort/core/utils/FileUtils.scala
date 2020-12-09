/*
 * MIT License
 *
 * Copyright (c) 2020 Jinho Ko
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package dpsort.core.utils

import scala.io.Source
import java.io.{BufferedInputStream, BufferedOutputStream, BufferedReader, BufferedWriter, File, FileInputStream, FileOutputStream, FileReader, FileWriter, PrintWriter}

import org.apache.logging.log4j.scala.Logging

/**
 * System PWD is set to $DPSORT_HOME
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
    val inputReader = new BufferedReader( new FileReader( filePath ) )
    try {
      val size = inputReader.lines().count().toInt
      inputReader.close
      size
    } finally {
      inputReader.close
    }
  }

  def fetchLinesFromFile( filePath: String, stIdx:Int, size:Int, lineSizeInBytes:Int  ) = {
    logger.debug(s"reading ${size} lines, where each line takes ${lineSizeInBytes} bytes")
    val inputStream = new BufferedInputStream( new FileInputStream( filePath ) )
    try {
      inputStream.skip( stIdx * lineSizeInBytes )
      val outputArr = Array.fill[Array[Byte]](size)( Array.fill[Byte](lineSizeInBytes)(Byte.MinValue) )
      for( lineIdx <- outputArr.indices )
        inputStream.read( outputArr(lineIdx), 0, lineSizeInBytes )
      outputArr
    } finally {
      inputStream.close
    }
  }

  def fetchLinesFromFile( filePath: String, lineSizeInBytes:Int ):Array[Array[Byte]] = {
    val inputReader = new BufferedReader( new FileReader( filePath ) )
    try {
      val size = inputReader.lines().count().toInt
      inputReader.close
      fetchLinesFromFile( filePath, 0, size, lineSizeInBytes )
    } finally {
      inputReader.close
    }
  }

  def writeLinesToFile(data: Array[Array[Byte]], path: String ) = {
    val outputStream = new BufferedOutputStream( new FileOutputStream( path ) )
    for ( line: Array[Byte] <- data ) {
      outputStream.write( line )
    }
    outputStream.close
  }

  def deleteFile( path:String ) = {
    val file = new File( path )
    if( checkIfFileExists(path) ) {
      file.delete()
    } else{
      logger.error("file deletion failed : file does not exists")
    }
  }

}