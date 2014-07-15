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

import java.util.{ArrayList, HashMap, LinkedList}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.json4s.JsonAST._

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * A SparkListener that publishes events to a Yarn Application Timeline Server.
 */
class YarnTimelineListener(private val sc: SparkContext, private val app: ApplicationReport)
  extends SparkListener with Logging {

  private val entity = new TimelineEntity()
  entity.setEntityType(YarnTimelineConstants.ENTITY_TYPE)
  entity.setEntityId(app.getApplicationId().toString())
  entity.setStartTime(app.getStartTime())
  entity.addPrimaryFilter("appName", sc.appName)
  entity.addPrimaryFilter("sparkUser", sc.sparkUser)
  entity.addOtherInfo("appName", sc.appName)
  entity.addOtherInfo("sparkUser", sc.sparkUser)
  entity.addOtherInfo("startTime", app.getStartTime())

  private val eventQueue = new LinkedBlockingQueue[TimelineEvent]()
  private val worker = new YarnTimelineWorker(sc.hadoopConfiguration, entity, eventQueue)
  private val workerThread = new Thread(worker, "YarnTimelineWorkerThread")
  workerThread.start()

  override def onStageSubmitted(event: SparkListenerStageSubmitted) = {
    // TODO: submission time not filled in?
    val timestamp = event.stageInfo.submissionTime.getOrElse(System.currentTimeMillis)
    enqueueEvent(event, timestamp)
  }

  override def onTaskStart(event: SparkListenerTaskStart) = {
    enqueueEvent(event, event.taskInfo.launchTime)
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    enqueueEvent(event, timestamp)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd) = {
    // TODO: event / task info do not contain end time?
    val endTime = System.currentTimeMillis
    enqueueEvent(event, endTime)
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate) = {
    enqueueEvent(event, System.currentTimeMillis)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted) = {
    // TODO: completion time not filled in?
    val timestamp = event.stageInfo.completionTime.getOrElse(System.currentTimeMillis)
    enqueueEvent(event, timestamp)
  }

  override def onJobStart(event: SparkListenerJobStart) = {
    // TODO: no timestamp for event?
    val startTime = System.currentTimeMillis
    enqueueEvent(event, startTime)
  }

  override def onJobEnd(event: SparkListenerJobEnd) = {
    // TODO: no timestamp for event?
    val endTime = System.currentTimeMillis
    enqueueEvent(event, endTime)
  }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    enqueueEvent(event, timestamp)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    enqueueEvent(event, timestamp)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    enqueueEvent(event, timestamp)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart) = {
    enqueueEvent(event, event.time)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd) = {
    entity.addOtherInfo("endTime", java.lang.Long.valueOf(event.time))
    enqueueEvent(event, event.time)

    if (workerThread != null) {
      logDebug("Waiting for ATS client to finish...")
      eventQueue.offer(worker.poison)
      workerThread.join()
    }
  }

  private def enqueueEvent(event: SparkListenerEvent, timestamp: Long) = {
    logTrace("Enqueueing timeline event: %s, %d".format(
      Utils.getFormattedClassName(event), timestamp))

    val properties = toJavaMap(JsonProtocol.sparkEventToJson(event).asInstanceOf[JObject].obj)

    val tevent = new TimelineEvent()
    tevent.setTimestamp(timestamp)
    tevent.setEventType(Utils.getFormattedClassName(event))
    tevent.setEventInfo(properties)
    eventQueue.offer(tevent)
  }

  private def toJavaObject(v: JValue): Object = v match {
    case JNothing => null
    case JNull => null
    case JString(s) => s
    case JDouble(num) => java.lang.Double.valueOf(num)
    case JDecimal(num) => num.bigDecimal
    case JInt(num) => java.lang.Long.valueOf(num.longValue)
    case JBool(value) => java.lang.Boolean.valueOf(value)
    case JObject(fields) => toJavaMap(fields)
    case JArray(vals) => {
      val list = new java.util.ArrayList[Object]()
      vals.foreach(x => list.add(toJavaObject(x)))
      list
    }
  }

  /**
   * Converts a json4s list of fields into a Java Map suitable for serialization by Jackson,
   * which is used by the ATS client library.
   */
  private def toJavaMap(fields: List[(String, JValue)]) = {
    val map = new HashMap[String, Object]()
    fields.foreach(f => map.put(f._1, toJavaObject(f._2)))
    map
  }

}

/**
 * Worker that handles sending events to the timeline service asynchronously. When a request fails,
 * events are not dropped; they are sent together with the next batch, in the hope that the timeline
 * server will recover. This assumes that events are well-formed and won't cause the timeline server
 * to reject them, since currently there is no way to identify offending events (other than sending
 * events one at a time).
 */
private class YarnTimelineWorker(
    private val conf: Configuration,
    private val entity: TimelineEntity,
    private val eventQueue: BlockingQueue[TimelineEvent])
    extends Runnable with Logging {

  val poison = new TimelineEvent()
  val maxEvents = 64
  val client = TimelineClient.createTimelineClient()

  override def run() = {
    try {
      client.init(conf)
      client.start()

      val events = new LinkedList[TimelineEvent]()
      entity.setEvents(events)

      var shutdown = false
      while (!shutdown) {
        if (events.size() < maxEvents) {
          events.add(eventQueue.take())
          eventQueue.drainTo(events, maxEvents - events.size())
        }
        if (events.peekLast() == poison) {
          shutdown = true
          events.removeLast()
        }
        if (!events.isEmpty() && post()) {
          events.clear()
        }
      }

      // After we're done with events, update the entity itself to indicate the
      // application has finished.
      entity.addPrimaryFilter("status", "finished")
      post()
    } catch {
      case e: Exception => logWarning("Error in timeline server client.", e)
    } finally {
      client.stop()
    }
  }

  private def post() = {
    if (!entity.getEvents().isEmpty()) {
      logTrace("Sending %d events (last = %s)".format(entity.getEvents().size(),
        entity.getEvents().get(entity.getEvents().size() - 1).getEventType()))
    }
    val response = client.putEntities(entity)
    if (response.getErrors() != null && !response.getErrors().isEmpty()) {
      for (e <- response.getErrors()) {
        logWarning("Error sending event to timeline server: %s, %s, %d".format(
          e.getEntityId(), e.getEntityType(), e.getErrorCode()))
      }
      false
    } else {
      true
    }
  }

}
