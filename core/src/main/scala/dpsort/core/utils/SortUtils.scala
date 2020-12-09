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

import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Sorting}
import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}

import org.apache.logging.log4j.scala.Logging

import dpsort.core.{KEY_OFFSET_BYTES, MutableRecordLines, PartFunc}
import dpsort.core.execution.BaseTask


object SortUtils extends Logging {

  private object KeyOrdering extends Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      // In order to sort by ascending order,
      // it returns positive when x > y
      assert( x.size == y.size )
      _compare( x.toList, y.toList )
    }

    def _compare(x: List[Byte], y: List[Byte] ): Int = {
      (x, y) match {
        case (xh :: xt, yh :: yt) => {
          if( xh > yh ) {1}
          else if ( xh < yh ) {-1}
          else { _compare(xt, yt) }
        }
        case (List(), List()) => { 0 }
      }
    }

  }

  def sortLines( lines: Array[Array[Byte]] ): Array[Array[Byte]] = {
    /* The algorithm is a in-place sorting algorithm
     * Thus no additional memory that exceeds the partition size
     * will be required
     * */
    logger.debug(s"in-place quicksort ${lines.size} lines")
    Sorting.quickSort(lines)( KeyOrdering )
    logger.debug(s"sort finished")
    lines
  }

  def sampleKeys( lines: Array[Array[Byte]], sRatio: Float, keyOffset: Int ): Array[Array[Byte]] = {
    logger.debug(s"sample from ${lines} lines, with sample ratio ${sRatio}")
    def getKeyFromLine( line: Array[Byte] ): Array[Byte] = line.slice(0, keyOffset)

    val rand = new Random( System.currentTimeMillis )
    val sampledKeys: Array[Array[Byte]] =
      lines.filter( _ => rand.nextFloat <= sRatio )       // Sample entries with filter
           .map( li => getKeyFromLine(li) )
    val outputKeys = sampledKeys.size match {             // should at least include 1 sample
      case 0 => Array[Array[Byte]]( getKeyFromLine(lines(0)) )
      case _ => sampledKeys
    }
    val actualSampleRatio = outputKeys.size.toFloat / lines.size
    logger.debug(s"sampled ${outputKeys.size} keys, actual sample ratio: ${actualSampleRatio}")
    outputKeys
  }

  def splitPartitions( inputFile:String, partFunc: PartFunc, partitions: Array[MutableRecordLines],
                       nLines:Int, lineSizeInBytes:Int ) = {

    val inputStream = new BufferedInputStream( new FileInputStream( inputFile ) )
    try {
      for( lineIdx <- 0 until nLines ) {
        val line = Array.fill[Byte]( lineSizeInBytes )( 0 )
        inputStream.read( line, 0, lineSizeInBytes )
        val keyIdx = partFunc.zipWithIndex
                  .filter( kv => { assert( kv._1._1.size == line.slice(0, KEY_OFFSET_BYTES).size && line.slice(0, KEY_OFFSET_BYTES).size == KEY_OFFSET_BYTES  ) ; KeyOrdering.compare( kv._1._1, line.slice(0, KEY_OFFSET_BYTES) ) >= 0 }) // at least one key is filtered
                  .head._2
        partitions(keyIdx).append(line)
      }
    } finally {
      inputStream.close
    }

  }

  def mergePartitions(inp1path: String, inp2path: String, outPath: String, lineSizeInBytes: Int) = {

    val inp1Stream = new BufferedInputStream( new FileInputStream(inp1path) )
    val inp2Stream = new BufferedInputStream( new FileInputStream(inp2path) )
    val outStream = new BufferedOutputStream( new FileOutputStream(outPath) )
    try {
      val line1 = Array.fill[Byte]( lineSizeInBytes )( 0 )
      val line2 = Array.fill[Byte]( lineSizeInBytes )( 0 )
      var inp1Eof = false
      var inp2Eof = false
      inp1Stream.read( line1, 0, lineSizeInBytes )
      inp2Stream.read( line2, 0, lineSizeInBytes )
      while( ! (inp1Eof && inp2Eof) ) {
        if( (!inp1Eof) && (!inp2Eof) ) {
          val compareVal = KeyOrdering.compare( line1, line2 )
          if( compareVal > 0 ) { // x > y
            outStream.write( line2 )
            val res2 = inp2Stream.read( line2, 0, lineSizeInBytes )
            if( res2 == -1 ) { inp2Eof = true }
          } else {                // x <= y
            outStream.write( line1 )
            val res1 = inp1Stream.read( line1, 0, lineSizeInBytes )
            if( res1 == -1 ) { inp1Eof = true }
          }
        } else {
          if( inp1Eof ) {
            outStream.write( line2 )
            val res2 = inp2Stream.read( line2, 0, lineSizeInBytes )
            if( res2 == -1 ) { inp2Eof = true }
          } else {
            outStream.write( line1 )
            val res1 = inp1Stream.read( line1, 0, lineSizeInBytes )
            if( res1 == -1 ) { inp1Eof = true }
          }
        }
      }
    } finally {
      inp1Stream.close()
      inp2Stream.close()
      outStream.close()
    }
  }

}
