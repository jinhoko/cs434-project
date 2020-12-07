package dpsort.core.utils

import dpsort.core.execution.BaseTask
import org.apache.logging.log4j.scala.Logging

import scala.util.Random
import scala.util.Sorting

object SortUtils extends Logging {

  private object KeyOrdering extends Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      // In order to sort by ascending order,
      // it returns positive when x > y
      _compare( x.toList, y. toList )
    }

    def _compare(x: List[Byte], y: List[Byte] ): Int = {
      (x, y) match {
        case (xh :: xt, yh :: yt) => {
          if( xh > yh ) 1
          else if ( xh < yh ) -1
          else _compare(xt, yt)
        }
        case (xh :: Nil, yh :: Nil) => {
          if ( xh > yh ) 1
          else if ( xh < yh ) -1
          else 0
        }
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

}
