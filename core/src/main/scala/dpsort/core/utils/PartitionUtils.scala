package dpsort.core.utils

import dpsort.core.storage.PartitionMeta
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable

object PartitionUtils extends Logging{

  def blockOffsetsGenerator( pM: PartitionMeta, bSize: Int ): Array[(Int, Int)] = {
    val startOffsets = 1 to pM.pLines by bSize
    val endOffsets = startOffsets.map( ofs => Array(ofs+bSize-1,pM.pLines).min )
    startOffsets.zip(endOffsets).toArray
  }

}
