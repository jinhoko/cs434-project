package dpsort.core.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable}
import scala.collection.mutable

import com.google.protobuf.ByteString
object SerializationUtils {

  def serializeObjectToByteArray( obj: Serializable ): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close()
    stream toByteArray
  }
  def serializeObjectToByteString( obj: Serializable ): ByteString = {
    ByteString copyFrom serializeObjectToByteArray(obj)
  }

  def deserializeByteArrayToObject[T]( byteArr: Array[Byte] ): T = {
    assert( byteArr.size != 0 )
    val stream = new ByteArrayInputStream( byteArr )
    val ois = new ObjectInputStream(stream)
    val outputObj = ois.readObject().asInstanceOf[T]
    ois.close()
    outputObj
  }
  def deserializeByteStringToObject[T]( byteStr: ByteString ): T = {
    assert( byteStr.size != 0 )
    deserializeByteArrayToObject( byteStr.toByteArray )
  }

}
