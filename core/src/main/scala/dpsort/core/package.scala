package dpsort

package object core {

  /* Definition on input data */
  type RecordLines = Array[Array[Byte]]

  val LINE_SIZE_BYTES = 100
  val KEY_OFFSET_BYTES = 10

  val MIN_KEY = (0 until 10).map(_ => ' '.toByte).toArray
  val MAX_KEY = (0 until 10).map(_ => '~'.toByte).toArray

}
