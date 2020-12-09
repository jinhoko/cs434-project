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

import dpsort.core.storage.PartitionMeta

@SerialVersionUID(100L)
final class Registry(ip: String,
                     port: Int,
                     shPort: Int,
                     input_files: Array[PartitionMeta]
                    ) extends Serializable {

  val IP: String = ip
  val PORT: Int = port
  val SHUFFLE_PORT: Int = shPort
  def IP_PORT: (String, Int) = (IP, PORT)
  def IP_SHPORT: (String, Int) = (IP, SHUFFLE_PORT)
  val INPUT_FILES: Array[PartitionMeta] = input_files
  // Following data will be determined in master
  var _WORKER_ID: Int = 0

  override def toString: String = {
    return s"[Registry : IP=${this.IP} PORT=${this.PORT} ]"
  }
}
