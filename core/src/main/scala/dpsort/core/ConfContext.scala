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
    handleInputProperty(property)
    val result = configMap.getOrDefault( property, null )
    if( result == null ) { throw new NoSuchElementException("invalid key") }
    result
  }

  override def get(property: String, default: String): String = {
    handleInputProperty(property)
    val result = configMap.getOrDefault( property, null )
    if( result == null ) { return default }
    result
  }

  override def set(property: String, value: String): Unit = {
    handleInputProperty(property)
    logger.debug(s"setting property ${property}:${value}")
    configMap.put(property, value)
  }

  def setByOverride(property: String, value: String) = {
    // TODO this should only work when key already exists
    // if not exists, raise exception
  }

  private def handleInputProperty(property: String) : Unit = {
    property match {
      case null => throw new NullPointerException("null key")
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

  protected def loadFromUserDefinedProperties(fileName: String) = {
    // TODO write to load from user file.
    // if file not found just return
    // call setOverride
    // exception handling and debug.
  }

}