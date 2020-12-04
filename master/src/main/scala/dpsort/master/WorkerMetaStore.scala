package dpsort.master

import dpsort.core.Registry
import dpsort.core.utils.IdUtils

import scala.collection.mutable


object WorkerMetaStore {

  private val workerMetaStore: mutable.Map[Int, Registry] = mutable.Map[Int, Registry]()

  def addRegistry( registry: Registry ): Int = {
    val newId: Int = IdUtils.genNewWorkerID( workerMetaStore.keySet.toSet )
    workerMetaStore += ( newId -> registry )
    newId
  }

  def isDistinctRegistry( registry: Registry ) = {
    ! workerMetaStore.valuesIterator.exists( _.IP_PORT == registry.IP_PORT )
  }
  def getWorkerNum: Int = workerMetaStore.size
  def getWaitingWorkersNum: Int = MasterParams.NUM_SLAVES_INT - WorkerMetaStore.getWorkerNum
}
