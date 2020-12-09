/*
 * MIT License
 *
 * Copyright (c) 2020 Jinho Ko
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package dpsort.core

import scala.io.Source
import java.util.NoSuchElementException
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.logging.log4j.scala.Logging


trait Conf {
  def get(property: String) : String
  def get(property: String, default: String) : String
  def set(property: String, value: String) : Unit
}

// NOTE : Contents of this class partially references that of
//        Apache Spark's SparkConf class ( handling config management )
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

  override def toString: String = {
    return configMap.toString
  }

}