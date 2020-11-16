package dpsort.master

import dpsort.core.Registry
import dpsort.core.utils.IdUtils

import scala.collection.mutable


object WorkerMetaStore {

  val workerMeta: mutable.Map[Int, Registry] = mutable.Map[Int, Registry]()

  def addRegistry( registry: Registry ): Int = {
    val newId: Int = IdUtils.genNewWorkerID( workerMeta.keySet.toSet )
    workerMeta += ( newId -> registry )
    newId
  }

  def getWorkerNum: Int = workerMeta.size
  def getWaitingWorkersNum: Int = MasterParams.NUM_SLAVES_INT - WorkerMetaStore.getWorkerNum
  // TODO write more
}
