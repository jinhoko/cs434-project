package dpsort.core.network

import dpsort.core.network.Channel
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable.{Map => MutableMap}

object ChannelMap extends Logging {

  // Key: (IP:PORT), Value: Trait
  private val channelMap:MutableMap[(String, Int),Channel] = MutableMap.empty

  def addChannel(key:(String, Int), channel:Channel ): Unit = {
    channelMap += (key -> channel)
    logger.debug(s"channel connection to ${key._1}:${key._2} established with type ${channel.toString}")
  }

  def getChannel( key:(String, Int) ): Channel = {
    val chnl = channelMap(key) // will throw exception if not exists.
    chnl
  }

}
