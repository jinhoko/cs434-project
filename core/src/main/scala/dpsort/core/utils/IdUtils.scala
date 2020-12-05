package dpsort.core.utils


object IdUtils {

  private var recentTaskID = 0
  private var recentPartID = 0

  def genNewWorkerID( idSet: Set[Int] ) : Int = {
    idSet.size match {
      case 0 => 1 // invariant
      case _ => idSet.max + 1
    }
  }

  def genNewTaskID(): Int = {
    recentTaskID += 1
    recentTaskID
  }

  def genNewPartID(): Int = {
    recentPartID += 1
    recentPartID
  }

  // TODO generate others

  //  def genNewPartitionID ( someparam(e.g. workerID?) ) : String = {
//
//  }

//  def genNewTaskID ( someparam(e.g. current stage?) ) : Int = {
//
//  }

}
