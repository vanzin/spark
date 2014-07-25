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

import java.io._
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelineEvent}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.json4s.JsonAST._

import org.apache.spark._
import org.apache.spark.scheduler.cluster.YarnTimelineClient
import org.apache.spark.scheduler._
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * Implementation of Spark's timeline client. This class will buffer application events in
 * temporary files and upload them asynchronously to the timeline server. This is done to
 * minimize the effect on the running application - the event bus threads do not get blocked
 * for long (assuming local disk i/o is reasonably fast), and memory usage is kept under
 * control.
 */
private[spark] class YarnTimelineClientImpl extends SparkListener with YarnTimelineClient
  with Logging {

  private var conf: SparkConf = null
  private val client = TimelineClient.createTimelineClient()

  // These variables are used to control which event file is currently being written to.
  private var batchSize = -1
  private var logDir: File = null
  private var index = 1
  private var currentCount = 0
  private var out: ObjectOutputStream = null
  @volatile private var entity: TimelineEntity = null

  // Synchronization between the event listener and the upload thread.
  @volatile private var nextAvailable = 0
  private var done = false
  private var currentUploadIndex = 0
  private var uploadThread: Thread = null
  private val lock = new Object()

  override def start(sc: SparkContext, appId: ApplicationId): Unit = {
    // Check that the configuration points at an AHS, otherwise the client code will
    // not be able to connect.
    val yarnConf = new YarnConfiguration(sc.hadoopConfiguration)
    if (!yarnConf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
      YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      logInfo("Yarn timeline service not available, disabling client.")
      return
    }

    this.conf = sc.getConf
    this.logDir = Utils.createTempDir()
    this.batchSize = conf.getInt("spark.yarn.timeline.batchSize", 64)

    entity = new TimelineEntity()
    entity.setEntityType(YarnTimelineConstants.ENTITY_TYPE)
    entity.setEntityId(appId.toString())

    client.init(yarnConf)
    client.start()

    uploadThread = new Thread(new Runnable() {
      override def run() = Utils.logUncaughtExceptions(uploadEvents())
    })
    uploadThread.setName("YarnTimelineClientUploader")
    uploadThread.setDaemon(true)
    uploadThread.start()

    sc.addSparkListener(this)
    logInfo("Yarn timeline client started.")
  }

  override def stop(): Unit = {
    if (out != null) {
      out.close()
    }
    lock.synchronized {
      done = true
      lock.notifyAll()
    }
    uploadThread.join()
    logInfo("Yarn timeline client finished.")
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted) = {
    // TODO: submission time not filled in?
    val timestamp = event.stageInfo.submissionTime.getOrElse(System.currentTimeMillis)
    recordEvent(event, timestamp)
  }

  override def onTaskStart(event: SparkListenerTaskStart) = {
    recordEvent(event, event.taskInfo.launchTime)
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd) = {
    // TODO: event / task info do not contain end time?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted) = {
    // TODO: completion time not filled in?
    val timestamp = event.stageInfo.completionTime.getOrElse(System.currentTimeMillis)
    recordEvent(event, timestamp)
  }

  override def onJobStart(event: SparkListenerJobStart) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onJobEnd(event: SparkListenerJobEnd) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD) = {
    // TODO: no timestamp for event?
    val timestamp = System.currentTimeMillis
    recordEvent(event, timestamp)
  }

  override def onApplicationStart(event: SparkListenerApplicationStart) = {
    val newEntity = copyEntity()
    newEntity.setStartTime(event.time)
    newEntity.addPrimaryFilter("appName", event.appName)
    newEntity.addPrimaryFilter("sparkUser", event.sparkUser)
    newEntity.addOtherInfo("appName", event.appName)
    newEntity.addOtherInfo("sparkUser", event.sparkUser)
    newEntity.addOtherInfo("startTime", event.time)
    this.entity = new TimelineEntity()

    recordEvent(event, event.time)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd) = {
    val newEntity = copyEntity()
    newEntity.addOtherInfo("endTime", java.lang.Long.valueOf(event.time))
    this.entity = newEntity
    recordEvent(event, event.time)
  }

  private def recordEvent(event: SparkListenerEvent, timestamp: Long): Unit = {
    if (out == null) {
      out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(
        new File(logDir, "event." + index))))
      index += 1
    }

    try {
      out.writeObject(new EventEntry(timestamp, Utils.getFormattedClassName(event),
        toJavaMap(JsonProtocol.sparkEventToJson(event).asInstanceOf[JObject].obj)))
      currentCount += 1
      if (currentCount == batchSize) {
        out.close()
        out = null
        currentCount = 0
        lock.synchronized {
          nextAvailable += 1
          lock.notifyAll()
        }
      }
    } catch {
      case e: Exception =>
        logWarning("Failed to process Spark event.", e)
        Utils.logUncaughtExceptions(out.close())
        out = null
    }
  }

  private def uploadEvents() = {
    val retryWait = conf.getInt("spark.yarn.timeline.retry_wait_ms", 1000)

    def upload(sleepOnFailure: Boolean): Boolean = {
      if (uploadNextBatch()) {
        currentUploadIndex += 1
        return true
      } else if (sleepOnFailure) {
        Thread.sleep(retryWait)
      }
      false
    }

    while (!done) {
      lock.synchronized {
        while (!done && currentUploadIndex == nextAvailable) {
          lock.wait()
        }
      }

      while (!done && currentUploadIndex < nextAvailable) {
        upload(false)
      }
    }

    // The application is done, but there might still be events to process. At this point, let's
    // not indefinitely retry, giving up after a few attempts.
    var tries = conf.getInt("spark.yarn.timeline.max_retries", 5)
    while (tries > 0 && currentUploadIndex < nextAvailable) {
      if (!upload(tries > 1)) {
        tries -= 1
      }
    }
  }

  /**
   * Load all events from the current batch to be uploaded, and try to send them to the timeline
   * server.
   *
   * @return Whether successfully uploaded the data.
   */
  private def uploadNextBatch() = {
    val eventLog = new ObjectInputStream(new FileInputStream(
      new File(logDir, "event." + currentUploadIndex)))
    var eof = false

    val events = new JArrayList[TimelineEvent](batchSize)
    try {
      while (!eof) {
        try {
          val entry = eventLog.readObject().asInstanceOf[EventEntry]
          val tevent = new TimelineEvent()
          tevent.setTimestamp(entry.timestamp)
          tevent.setEventType(entry.eventType)
          tevent.setEventInfo(entry.eventInfo)
          events.add(tevent)
        } catch {
          case e: EOFException => eof = true
          case e: Exception =>
            logWarning("Error reading event from cached file.", e)
            throw e
        }
      }
    } finally {
      Utils.logUncaughtExceptions(eventLog.close())
    }

    val localEntity = entity
    localEntity.setEvents(events)

    val response = client.putEntities(localEntity)
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
    val map = new JHashMap[String, Object]()
    fields.foreach(f => map.put(f._1, toJavaObject(f._2)))
    map
  }

  private def copyEntity() = {
    val newEntity = new TimelineEntity()
    newEntity.setEntityType(entity.getEntityType())
    newEntity.setEntityId(entity.getEntityId())
    newEntity.setStartTime(entity.getStartTime())
    newEntity.setRelatedEntities(copyMap(entity.getRelatedEntities()))
    newEntity.setPrimaryFilters(copyMap(entity.getPrimaryFilters()))
    newEntity.setOtherInfo(copyMap(entity.getOtherInfo()))
    newEntity
  }

  private def copyMap[K: ClassTag, V: ClassTag](map: JMap[K, V]): JMap[K, V] =
    if (map != null) new JHashMap(map) else null

}

private class EventEntry(val timestamp: Long, val eventType: String,
  val eventInfo: JMap[String, Object]) extends Serializable {

}
