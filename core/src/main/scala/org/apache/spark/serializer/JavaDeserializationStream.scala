/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.serializer

import java.io._

import com.pingcap.tispark.utils.ReflectionUtil

import scala.reflect.ClassTag

private[spark] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
    extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] = {
      try {
        doResolveClass1(desc)
      } catch {
        case e: Throwable =>
          try {
            Class.forName(desc.getName, false, ReflectionUtil.classLoader)
          } catch {
            case _: Throwable => throw e
          }
      }
    }

    private def doResolveClass1(desc: ObjectStreamClass): Class[_] = {
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
    }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close(): Unit = { objIn.close() }
}

private object JavaDeserializationStream {
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Void])
}
