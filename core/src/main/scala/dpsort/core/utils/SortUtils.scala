package dpsort.core.utils

import dpsort.core.execution.BaseTask

import scala.util.Sorting

object SortUtils {


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
          else if ( xh < yh) -1
          else _compare(xt, yt)
        }
        case (xh :: Nil, yh :: Nil) => {
          if (xh > yh) 1
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
    Sorting.quickSort(lines)( KeyOrdering )
    lines
  }
}
