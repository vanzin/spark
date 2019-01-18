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

package org.apache.spark.internal.config

private[spark] object Shuffle {

  val DFS_IO_PATH = ConfigBuilder("spark.shuffle.dfs.path")
    .doc("Used internally to define where to write shuffle files.")
    .internal()
    .stringConf
    .createOptional

  val DFS_IO_BASE_PATH = ConfigBuilder("spark.shuffle.dfs.basePath")
    .doc("Base path where to store shuffle files in the distributed fs.")
    .stringConf
    .createOptional

}
