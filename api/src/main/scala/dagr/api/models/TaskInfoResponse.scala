/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

package dagr.api.models

import upickle.Js
import upickle.default.{Reader, Writer}

case class TaskInfoResponse(infos: Seq[TaskInfo])

object TaskInfoResponse {
  implicit class InfoId(info: TaskInfo) {
    def idStr: String = info.id.map(_.toString).getOrElse("None")
  }

  implicit val query2Writer: Writer[TaskInfoResponse] = Writer[TaskInfoResponse] {
    response: TaskInfoResponse =>
      val tuples = response.infos.map { info =>
        (info.idStr, TaskInfo.query2Writer.write(info))
      }
      Js.Obj(tuples:_*)
  }

  implicit val query2Reader: Reader[TaskInfoResponse] = Reader[TaskInfoResponse] {
    case obj: Js.Obj =>
      val infos = obj.value.map { case (id, value) =>
        val info = TaskInfo.query2Reader.read(value)
        if (info.idStr != id) throw upickle.Invalid.Data(value, s"Task ids did not match ('$id' != '${info.idStr}'")
        info
      }
      TaskInfoResponse(infos=infos)
  }
}

