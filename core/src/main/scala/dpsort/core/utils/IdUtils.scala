package dpsort.core.utils


object IdUtils {

  def genNewWorkerID( idSet: Set[Int] ) : Int = {
    idSet.size match {
      case 0 => 1 // invariant
      case _ => idSet.max + 1
    }
  }

  // TODO generate others

  //  def genNewPartitionID ( someparam(e.g. workerID?) ) : String = {
//
//  }

//  def genNewTaskID ( someparam(e.g. current stage?) ) : Int = {
//
//  }

}
