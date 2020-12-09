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

package dpsort.core.network

import scala.collection.mutable.{Map => MutableMap}
import java.util.concurrent.locks.ReentrantLock

import org.apache.logging.log4j.scala.Logging

import dpsort.core.network.Channel


object ChannelMap extends Logging {

  // Key: (IP:PORT), Value: Trait
  private val channelMap: MutableMap[(String, Int),Channel] = MutableMap.empty
  private val cmLock: ReentrantLock = new ReentrantLock()

  def addChannel(key:(String, Int), channel:Channel ): Unit = {
    channelMap += (key -> channel)
    logger.info(s"channel connection to ${key._1}:${key._2} established with type ${channel.toString}")
  }

  def getChannel( key:(String, Int) ): Channel = {
    val chnl = channelMap(key) // will throw exception if not exists.
    chnl
  }

}
