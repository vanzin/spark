/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn.timeline

import java.util.{ArrayList => JArrayList, Collection => JCollection, HashMap => JHashMap,
  Map => JMap}

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent
import org.json4s.JsonAST._

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol

private object YarnTimelineUtils {
  val ENTITY_TYPE = "SparkApplication"

  /**
   * Converts a Java object to its equivalent json4s representation.
   */
  def toJValue(obj: Object): JValue = obj match {
    case str: String => JString(str)
    case dbl: java.lang.Double => JDouble(dbl)
    case dec: java.math.BigDecimal => JDecimal(dec)
    case int: java.lang.Integer => JInt(BigInt(int))
    case long: java.lang.Long => JInt(BigInt(long))
    case bool: java.lang.Boolean => JBool(bool)
    case map: JMap[_, _] =>
      val jmap = map.asInstanceOf[JMap[String, Object]]
      JObject(jmap.entrySet().map { e => (e.getKey() -> toJValue(e.getValue())) }.toList)
    case array: JCollection[_] =>
      JArray(array.asInstanceOf[JCollection[Object]].map(o => toJValue(o)).toList)
    case null => JNothing
  }

  /**
   * Converts a JValue into its Java equivalent.
   */
  def toJavaObject(v: JValue): Object = v match {
    case JNothing => null
    case JNull => null
    case JString(s) => s
    case JDouble(num) => java.lang.Double.valueOf(num)
    case JDecimal(num) => num.bigDecimal
    case JInt(num) => java.lang.Long.valueOf(num.longValue)
    case JBool(value) => java.lang.Boolean.valueOf(value)
    case obj: JObject => toJavaMap(obj)
    case JArray(vals) => {
      val list = new JArrayList[Object]()
      vals.foreach(x => list.add(toJavaObject(x)))
      list
    }
  }

  /**
   * Converts a json4s list of fields into a Java Map suitable for serialization by Jackson,
   * which is used by the ATS client library.
   */
  def toJavaMap(obj: JObject): JHashMap[String, Object] = {
    val map = new JHashMap[String, Object]()
    obj.obj.foreach(f => map.put(f._1, toJavaObject(f._2)))
    map
  }

  def toSparkEvent(event: TimelineEvent): SparkListenerEvent = {
    JsonProtocol.sparkEventFromJson(toJValue(event.getEventInfo()))
  }

}
