package dpsort

package object worker {

  /* Definition on input data */
  type RecordLines = Array[Array[Byte]]

  val LINE_SIZE_BYTES = 100
  val KEY_OFFSET_BYTES = 10

}
