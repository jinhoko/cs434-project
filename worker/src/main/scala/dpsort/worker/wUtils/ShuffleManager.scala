package dpsort.worker.wUtils

import org.apache.logging.log4j.scala.Logging

import dpsort.worker.WorkerConf._
import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor}

import com.google.protobuf.ByteString
import dpsort.core.execution.{BaseTask, TaskType}
import dpsort.core.network.{ChannelMap, ResponseMsg, TaskReportMsg}
import dpsort.core.network.ResponseMsg._
import dpsort.core.utils.SerializationUtils._
import dpsort.core.network.TaskReportMsg.TaskResultType
import dpsort.worker
import dpsort.worker.execution.ExecCtxtFetcher

/* Handles both shuffle output and input */
object ShuffleManager extends Logging {

  var numOngoingShuffles = 0

  private val maxShuffleConnections = get("dpsort.worker.maxShuffleConnections").toInt
  private def numShuffleConnectionsLeft = maxShuffleConnections - numOngoingShuffles

  private def isShuffleInAvailable: Boolean = numShuffleConnectionsLeft > 0

  /* Shuffle In */
  def shuffleRequestHandler = {
    // numOngoing -= 1
    // generate response
    // response.
  }

  /* Shuffle Out */
  def shuffleOut( ) = {
    // generate status list (except mine)
    // def termcond = all finish

    // iterate ( termcond )
    // if not submitted request shuffle
    // if answer ok,
    // mark sent
    //send to data
    // response oncomplete mark ok => 가능한지?
    // else go on
  }

}
