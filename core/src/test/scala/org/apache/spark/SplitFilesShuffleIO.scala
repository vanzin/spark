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

package org.apache.spark

import java.io.{File, FileInputStream, IOException, InputStream}
import java.nio.file.{Files, Paths, StandardOpenOption}

import javax.ws.rs.core.UriBuilder
import org.apache.commons.io.IOUtils

import org.apache.spark.shuffle.api._
import org.apache.spark.util.Utils

class SplitFilesShuffleIO(conf: SparkConf) extends ShuffleDataIO {

  private val shuffleDir = Utils.createTempDir("test-shuffles-pluggable-dir")

  override def initialize(): Unit = {}

  override def readSupport(): ShuffleReadSupport = (appId: String, shuffleId: Int, mapId: Int) => {
    reduceId: Int => {
      new FileInputStream(resolvePartitionFile(appId, shuffleId, mapId, reduceId))
    }
  }

  override def writeSupport(): ShuffleWriteSupport = {
    (appId: String, shuffleId: Int, mapId: Int) => new ShuffleMapOutputWriter {
      override def newPartitionWriter(partitionId: Int): ShufflePartitionWriter = {
        new ShufflePartitionWriter {
          override def appendBytesToPartition(streamReadingBytesToAppend: InputStream): Unit = {
            val shuffleFile = resolvePartitionFile(appId, shuffleId, mapId, partitionId)
            if (!shuffleFile.getParentFile.isDirectory && !shuffleFile.getParentFile.mkdirs()) {
              throw new IllegalStateException(
                s"Failed to make parent dir ${shuffleFile.getParent}")
            }
            Files.write(
              shuffleFile.toPath,
              IOUtils.toByteArray(streamReadingBytesToAppend),
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND)
          }

          override def commitAndGetTotalLength(): Long =
            resolvePartitionFile(appId, shuffleId, mapId, partitionId).length

          override def abort(failureReason: Exception): Unit = {}
        }
      }

      override def commitAllPartitions(): Unit = {}

      override def abort(exception: Exception): Unit = {}
    }
  }

  private def resolvePartitionFile(
      appId: String, shuffleId: Int, mapId: Int, reduceId: Int): File = {
    Paths.get(UriBuilder.fromUri(shuffleDir.toURI)
      .path(appId)
      .path(shuffleId.toString)
      .path(mapId.toString)
      .path(reduceId.toString)
      .build()
      .getPath)
      .toFile
  }
}
