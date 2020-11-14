package dpsort.master

import org.apache.logging.log4j.scala.Logging

import dpsort.master.MasterConf

import java.io._
import dpsort.core.execution._

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("dpsort master starting")
    // Load Conf object

    // Start service
    MasterTaskServer.startServer()
    HeartBeatServer.startServer()

    val a = new TerminateTask(1, TaskStatus.SUBMITTED, 0, 0)
    val oos = new ObjectOutputStream(new FileOutputStream("/tmp/nflx.txt"))
    oos.writeObject(a)
    oos.close

    val ois = new ObjectInputStream(new FileInputStream("/tmp/nflx.txt"))
    val aaaa = 1
    val aaaaa = aaaa match{
      case 1 => ois.readObject.asInstanceOf[TerminateTask]
      case 2 => ois.readObject.asInstanceOf[GenBlockTask]
    }
    ois.close

    println(aaaaa.a)
    println(aaaaa.aa.size)

    // TODO execute master context

    // Stop services
    MasterTaskServer.stopServer()
    HeartBeatServer.stopServer()
    logger.info("dpsort master finished")
  }
}
