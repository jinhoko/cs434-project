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

package dpsort.master

import scala.collection.mutable

import dpsort.core.Registry
import dpsort.core.utils.IdUtils


object WorkerMetaStore {

  private val workerMetaStore: mutable.Map[Int, Registry] = mutable.Map[Int, Registry]()

  def addRegistry( registry: Registry ): Int = {
    val newId: Int = IdUtils.genNewWorkerID( workerMetaStore.keySet.toSet )
    workerMetaStore += ( newId -> registry )
    newId
  }

  def isDistinctRegistry( registry: Registry ) = {
    (! workerMetaStore.valuesIterator.exists( _.IP_PORT == registry.IP_PORT )) &&
    (! workerMetaStore.valuesIterator.exists( _.SHUFFLE_PORT == registry.SHUFFLE_PORT ))
  }
  def getWorkerNum: Int = workerMetaStore.size
  def getWaitingWorkersNum: Int = MasterParams.NUM_SLAVES_INT - WorkerMetaStore.getWorkerNum

  def getWorkerIpPort(wid:Int): (String, Int) = workerMetaStore(wid).IP_PORT
  def getWorkerShuffleIPPort(wid: Int): (String, Int) = workerMetaStore(wid).IP_SHPORT

}
