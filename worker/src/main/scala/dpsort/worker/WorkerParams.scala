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

package dpsort.worker

import org.apache.logging.log4j.scala.Logging

import dpsort.core.Params
import dpsort.core.utils.FileUtils.getFilesInDirectory

object WorkerParams extends Params with Logging {

  var MASTER_IP_STR: String = ""
  var MASTER_PORT_INT: Int = 0
  var INPUT_FILES_STRARR: Array[String] = Array("")
  var OUTPUT_DIR_STR: String = ""


  protected override def setParams ( params: Array[String] ): Unit = {
    MASTER_IP_STR = params(0)
    MASTER_PORT_INT = params(1).toInt
    OUTPUT_DIR_STR = params(2)
    INPUT_FILES_STRARR = params.drop(3)
      .flatMap( getFilesInDirectory ).distinct
  }

  lazy val MASTER_IP_PORT:(String, Int) = (MASTER_IP_STR, MASTER_PORT_INT)

}