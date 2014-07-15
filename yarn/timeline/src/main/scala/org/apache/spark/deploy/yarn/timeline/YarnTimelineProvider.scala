/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import java.io.FileNotFoundException
import java.net.URI
import java.util.{Collection => JCollection, Map => JMap}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.sun.jersey.api.client.{Client, ClientResponse, WebResource}
import com.sun.jersey.api.client.config.{ClientConfig, DefaultClientConfig}
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntities, TimelineEvents}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider
import org.json4s.JsonAST._

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.history.{ApplicationHistoryInfo, ApplicationHistoryProvider}
import org.apache.spark.scheduler.{ApplicationEventListener, SparkListenerBus}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.JsonProtocol

/**
 * Provider that fetches Spark history data from a running Yarn Application Timeline Server.
 *
 * Note that, currently, there is no client API for reading from the AHS, so this class uses
 * a JAX-RS client directly. It borrows some code from Yarn for the client setup.
 */
private class YarnTimelineProvider(conf: SparkConf) extends ApplicationTimelineProvider
  with Logging {

  private val yarnConf = new YarnConfiguration()

  // Copied from Yarn's TimelineClientImpl.java.
  private val RESOURCE_URI_STR = s"/ws/v1/timeline/${YarnTimelineConstants.ENTITY_TYPE}"

  if (!yarnConf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
    throw new IllegalStateException("Timeline service not enabled in configuration.")
  }

  private val timelineUri = {
    val isHttps = YarnConfiguration.useHttps(yarnConf)
    val addressKey =
      if (isHttps) {
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS
      } else {
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS
      }
    val protocol = if (isHttps) "https://" else "http://"
    URI.create(s"$protocol${yarnConf.get(addressKey)}$RESOURCE_URI_STR")
  }

  private val client = {
    val cc = new DefaultClientConfig()
    // Note: this class is "interface audience private" but since there is no public API
    // for the timeline server, this makes it much easier to talk to it.
    cc.getClasses().add(classOf[YarnJacksonJaxbJsonProvider])
    Client.create(cc)
  }

  logInfo(s"Using Yarn history server at $timelineUri")

  override def getListing(): Seq[ApplicationTimelineInfo] = {
    val resource = client.resource(timelineUri)
    val entities = resource
      .queryParam("primaryFilter", "status:finished")
      .queryParam("fields", "primaryFilters,otherInfo")
      .accept(MediaType.APPLICATION_JSON)
      .get(classOf[ClientResponse])
      .getEntity(classOf[TimelineEntities])

    entities.getEntities().map(e =>
      ApplicationTimelineInfo(e.getEntityId(),
        e.getOtherInfo().get("appName").asInstanceOf[String],
        e.getOtherInfo().get("startTime").asInstanceOf[Number].longValue,
        e.getOtherInfo().get("endTime").asInstanceOf[Number].longValue,
        e.getOtherInfo().get("endTime").asInstanceOf[Number].longValue,
        e.getOtherInfo().get("sparkUser").asInstanceOf[String])
    )
  }

  override def getAppUI(appId: String): SparkUI = {
    val eventsUri = timelineUri.resolve(s"${timelineUri.getPath()}/events")
    val resource = client.resource(eventsUri)
      .queryParam("entityId", appId)
      .accept(MediaType.APPLICATION_JSON)

    val events = resource.get(classOf[ClientResponse])
      .getEntity(classOf[TimelineEvents])

    val bus = new SparkListenerBus() { }
    val appListener = new ApplicationEventListener()
    bus.addListener(appListener)

    val ui = {
      val conf = this.conf.clone()
      val appSecManager = new SecurityManager(conf)
      new SparkUI(conf, appSecManager, bus, appId, "/history/" + appId)
    }

    events.getAllEvents().foreach { entityEvents =>
      entityEvents.getEvents().reverse.foreach { e =>
        val sparkEvent = toJValue(e.getEventInfo())
        bus.postToAll(JsonProtocol.sparkEventFromJson(sparkEvent))
      }
    }

    val uiAclsEnabled = conf.getBoolean("spark.history.ui.acls.enable", false)
    ui.getSecurityManager.setUIAcls(uiAclsEnabled)
    ui.getSecurityManager.setViewAcls(appListener.sparkUser, appListener.viewAcls)
    ui
  }

  override def getConfig(): Map[String, String] =
    Map(("Yarn Application Timeline Server" -> timelineUri.resolve("/").toString()))

  override def stop(): Unit = client.destroy()

  private def toJValue(obj: Object): JValue = obj match {
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

}
