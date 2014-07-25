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
import org.apache.spark.deploy.yarn.{ApplicationMaster, ApplicationMasterArguments}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.IntParam

private[spark] trait YarnTimelineClient {

  def start(sc: SparkContext, appId: ApplicationId): Unit

  def stop(): Unit

}

private[spark] object YarnTimelineClient extends Logging {

  def apply(sc: SparkContext): Option[YarnTimelineClient] = {
    if (sc.getConf.getBoolean("spark.yarn.timeline.enabled", true)) {
      val listenerClass = "org.apache.spark.deploy.yarn.timeline.YarnTimelineClientImpl"
      try {
        var client = Class.forName(listenerClass)
          .newInstance()
          .asInstanceOf[YarnTimelineClient]
        Some(client)
      } catch {
        case e: Exception =>
          logInfo("Cannot instantiate Yarn timeline client.", e)
          None
      }
    } else {
      logInfo("Yarn timeline client disabled or unavailable.")
      None
    }
  }

}
