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

package org.apache.spark.shuffle.dfs;

import java.io.InputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.storage.ShuffleLocation;

class DFSShuffleReadSupport implements ShuffleReadSupport {

  private final FileSystem fs;
  private final Path path;

  DFSShuffleReadSupport(FileSystem fs, Path path) {
    this.fs = fs;
    this.path = path;
  }

  @Override
  public ShufflePartitionReader newPartitionReader(String appId, int shuffleId, int mapId) {
    return new PartitionReader(shuffleId, mapId);
  }

  private class PartitionReader implements ShufflePartitionReader {

    private final int shuffleId;
    private final int mapId;

    PartitionReader(int shuffleId, int mapId) {
      this.shuffleId = shuffleId;
      this.mapId = mapId;
    }

    @Override
    public InputStream fetchPartition(int reduceId, Optional<ShuffleLocation> location)
        throws IOException {
      String shuffleFile = String.format("shuffle-%d/%d/%d", shuffleId, reduceId, mapId);
      Path shufflePath = new Path(path, shuffleFile);
      return fs.open(shufflePath);
    }

  }

}
