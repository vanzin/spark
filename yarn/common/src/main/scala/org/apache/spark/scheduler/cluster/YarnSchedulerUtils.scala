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

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.records.ApplicationId

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler.SparkListener

private object YarnSchedulerUtils extends Logging {

  def addTimelineListener(sc: SparkContext, appId: ApplicationId, startTime: Long) {
    // If Yarn history is enabled and the listener class exists, then enable it to feed data
    // to the server.
    //
    // Also check that the configuration points at an AHS, otherwise the client code will
    // not be able to connect. Since we can't directy depend on the constants from the Yarn
    // classes (due to compatibility with older versions), the config name is hardcoded.
    if (sc.getConf.getBoolean("spark.yarn.history.enabled", true) &&
      sc.hadoopConfiguration.get("yarn.timeline-service.webapp.address") != null) {
      val listenerClass = "org.apache.spark.deploy.yarn.history.YarnTimelineListener"
      try {
        var listener = Class.forName(listenerClass)
          .getConstructor(classOf[SparkContext], classOf[ApplicationId], classOf[Long])
          .newInstance(sc, appId, java.lang.Long.valueOf(startTime))
          .asInstanceOf[SparkListener]
        sc.addSparkListener(listener)
      } catch {
        case e: Exception => logDebug("Cannot instantiate Yarn history listener.", e)
      }
    }
  }

}
