package dpsort.core

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import scala.io.Source
import java.util.NoSuchElementException

import org.apache.logging.log4j.scala.{Logger, Logging}

trait Conf {
  def get(property: String) : String
  def get(property: String, default: String) : String
  def set(property: String, value: String) : Unit
}

// NOTE : Contents of this class partially references that of
//        Apache Spark's SparkConf class ( handling config management )
// TODO Write more!
abstract class ConfContext extends Conf with Logging {

  private[this] val configMap = new ConcurrentHashMap[String, String]()

  override def get(property: String): String = {
    handleInputPropertyValue(property)
    val result = configMap.getOrDefault( property, null )
    if( result == null ) { throw new NoSuchElementException("invalid key") }
    result
  }

  override def get(property: String, default: String): String = {
    handleInputPropertyValue(property)
    val result = configMap.getOrDefault( property, null )
    if( result == null ) { return default }
    result
  }

  override def set(property: String, value: String): Unit = {
    handleInputPropertyValue(property, value)
    logger.debug(s"setting property ${property} => ${value}")
    configMap.put(property, value)
  }

  def setByOverride(property: String, value: String): Unit = {
    handleInputPropertyValue(property, value)
    try {
      get(property) // if key invalid, it wil throw NoSuchElementException
    } catch {
      case v: NoSuchElementException => return
    }
    set(property, value)
  }

  private def handleInputPropertyValue(property: String, value: String = "") : Unit = {
    (property, value) match {
      case (null, null) => throw new NullPointerException("null key and value")
      case (null, _) => throw new NullPointerException("null key")
      case (_, null) => throw new NullPointerException("null value")
      case _ => return
    }
  }

  protected def loadFromResourceProperties( fileName : String ) = {
    val propertiesResource = getClass.getResource(fileName)
    assert( propertiesResource != null )
    val source = Source.fromURL(propertiesResource)
    val properties = new Properties()
    properties.load(source.bufferedReader())
    // load properties into configMap
    properties.stringPropertyNames().toArray(Array[String]())
      .map( pName => set(pName, properties.getProperty(pName)) )

  }

  protected def loadFromUserDefinedProperties( fileName: String ) = {
    val source = Source.fromFile( fileName )
    val properties = new Properties()
    properties.load(source.bufferedReader())
    // load properties into configMap
    properties.stringPropertyNames().toArray(Array[String]())
      .map( pName => setByOverride(pName, properties.getProperty(pName)) )
  }

}