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

import scala.collection.JavaConversions._

import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.timeline.{TimelineEntity, TimelinePutResponse}
import org.apache.hadoop.yarn.client.api.TimelineClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler._
import org.apache.spark.util.JsonProtocol

class YarnTimelineClientSuite extends FunSuite with BeforeAndAfter with Matchers {

  private val environmentUpdate = SparkListenerEnvironmentUpdate(Map[String, Seq[(String, String)]](
    "JVM Information" -> Seq(("GC speed", "9999 objects/s"), ("Java home", "Land of coffee")),
    "Spark Properties" -> Seq(("Job throughput", "80000 jobs/s, regardless of job type")),
    "System Properties" -> Seq(("Username", "guest"), ("Password", "guest")),
    "Classpath Entries" -> Seq(("Super library", "/tmp/super_library"))
  ))
  private val applicationStart = SparkListenerApplicationStart("myapp", 42L, "bob")
  private val applicationEnd = SparkListenerApplicationEnd(84L)

  private var timelineClient: TimelineClient = _
  private var response: TimelinePutResponse = _
  private var appId: ApplicationId = _

  before {
    timelineClient = mock(classOf[TimelineClient])
    response = mock(classOf[TimelinePutResponse])
    when(timelineClient.putEntities(any(classOf[TimelineEntity]))).thenReturn(response)

    appId = mock(classOf[ApplicationId])
    when(appId.toString()).thenReturn("someapp")
  }

  test("batch processing of Spark listener events") {
    val sc = createContext(new SparkConf())
    postEvents(sc)
    verify(timelineClient).putEntities(any(classOf[TimelineEntity]))
  }

  test("multiple Spark listener event batches") {
    val sc = createContext(new SparkConf().set("spark.yarn.timeline.batchSize", "1"))
    postEvents(sc)

    val captor = ArgumentCaptor.forClass(classOf[TimelineEntity])
    verify(timelineClient, times(3)).putEntities(captor.capture())

    val update1 = captor.getAllValues().get(0)
    val uploadedEnvUpdate = toSparkEvent(update1)
    uploadedEnvUpdate should be (environmentUpdate)

    val update2 = captor.getAllValues().get(1)
    val uploadedAppStart = toSparkEvent(update2)
    uploadedAppStart should be (applicationStart)
    update2.getPrimaryFilters().get("appName").toSeq should be (Seq(applicationStart.appName))
    update2.getPrimaryFilters().get("sparkUser").toSeq should be (Seq(applicationStart.sparkUser))
    update2.getStartTime() should be (applicationStart.time)

    val update3 = captor.getAllValues().get(2)
    val uploadedAppEnd = toSparkEvent(update3)
    uploadedAppEnd should be (applicationEnd)
  }

  test("retry upload on failure") {
    when(timelineClient.putEntities(any(classOf[TimelineEntity])))
      .thenThrow(new RuntimeException("oops"))
      .thenReturn(response)

    val sc = createContext(new SparkConf().set("spark.yarn.timeline.batchSize", "1"))
    postEvents(sc)
    verify(timelineClient, times(4)).putEntities(any(classOf[TimelineEntity]))
  }

  private def postEvents(sc: SparkContext) = {
    val client = spy(new YarnTimelineClientImpl())
    doReturn(timelineClient).when(client).creteTimelineClient()

    try {
      client.start(sc, appId)
      client.onEnvironmentUpdate(environmentUpdate)
      client.onApplicationStart(applicationStart)
      client.onApplicationEnd(applicationEnd)
    } finally {
      client.stop()
    }
  }

  private def createContext(conf: SparkConf) = {
    val sc = new SparkContext(conf.setMaster("local").setAppName("myapp"))
    sc.hadoopConfiguration.set(YarnConfiguration.TIMELINE_SERVICE_ENABLED, "true")
    sc
  }

  private def toSparkEvent(entity: TimelineEntity) = {
    assert(entity.getEvents().size() === 1)
    YarnTimelineUtils.toSparkEvent(entity.getEvents().get(0))
  }

}
