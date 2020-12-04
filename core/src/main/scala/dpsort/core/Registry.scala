package dpsort.core

import dpsort.core.storage.PartitionMeta

@SerialVersionUID(100L)
final class Registry(ip: String,
                     port: Int,
                     input_files: Array[PartitionMeta]
                    ) extends Serializable {

  val IP: String = ip
  val PORT: Int = port
  val IP_PORT: (String, Int) = (IP, PORT)
  val INPUT_FILES: Array[PartitionMeta] = input_files
  // TODO worker heartbeat IP, etc...
  // Following data will be determined in master
  var _WORKER_ID: Int = 0

  override def toString: String = {
    return s"[Registry : IP=${this.IP} PORT=${this.PORT} ]"
  }
}
