package dpsort.core

@SerialVersionUID(100L)
final class Registry(ip: String,
                     port: Int,
                     input_files: Array[String]
                    ) extends Serializable {

  val IP: String = IP
  val PORT: Int = PORT
  val INPUT_FILES: Array[String] = input_files
  // TODO inputfiles string array가 아니라 PartitionMeta의 object를 보내줘야 함.

  // Following data will be determined in master
  var _WORKER_ID: Int = 0
}
