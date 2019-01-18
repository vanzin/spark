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

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.api.CommittedPartition;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.apache.spark.storage.ShuffleLocation;

class DFSShuffleWriteSupport implements ShuffleWriteSupport {

  private final FileContext fc;
  private final FileSystem fs;
  private final Path path;

  DFSShuffleWriteSupport(Configuration conf, FileSystem fs, Path path) throws IOException {
    this.fs = fs;
    this.fc = FileContext.getFileContext(path.toUri(), conf);
    this.path = path;
  }

  @Override
  public ShuffleMapOutputWriter newMapOutputWriter(String appId, int shuffleId, int mapId) {
    return new MapOutputWriter(shuffleId, mapId);
  }

  private class MapOutputWriter implements ShuffleMapOutputWriter {

    private final int shuffleId;
    private final int mapId;

    MapOutputWriter(int shuffleId, int mapId) {
      this.shuffleId = shuffleId;
      this.mapId = mapId;
    }

    @Override
    public ShufflePartitionWriter newPartitionWriter(int partitionId) throws IOException {
      String shuffleOutput = String.format("shuffle-%d/%d/%d", shuffleId, partitionId, mapId);
      return new PartitionWriter(new Path(path, shuffleOutput));
    }

    @Override
    public void commitAllPartitions() {
      // Nothing to do. Handled by the partition writers.
    }

    @Override
    public void abort(Exception exception) {
      // Nothing to do. Handled by the partition writers.
    }

  }

  private class PartitionWriter implements ShufflePartitionWriter {

    private final Path output;
    private CountingOutputStream stream;

    PartitionWriter(Path output) {
      this.output = output;
    }

    @Override
    public OutputStream openPartitionStream() throws IOException {
      stream = new CountingOutputStream(fs.create(getTempPath()));
      return stream;
    }

    @Override
    public CommittedPartition commitPartition() throws IOException {
      Preconditions.checkState(stream != null);
      stream.close();

      Path tmp = getTempPath();
      try {
        fc.rename(tmp, output);

        final long length = stream.getByteCount();
        return new CommittedPartition() {
          @Override
          public long length() {
            return length;
          }

          @Override
          public Optional<ShuffleLocation> shuffleLocation() {
            return Optional.empty();
          }
        };
      } catch (FileAlreadyExistsException faee) {
        // Another speculative task won the race, so just ignore this output.
        fs.delete(tmp, true);
        return null;
      }
    }

    @Override
    public void abort(Exception failureReason) throws IOException {
      if (stream != null) {
        stream.close();
        fs.delete(getTempPath(), true);
      }
    }

    private Path getTempPath() {
      TaskContext tc = TaskContext.get();
      Path tmp = new Path(output.toString() + ".tmp." + tc.taskAttemptId());
      System.out.printf("tmp = %s, out = %s\n", tmp, output);
      return tmp;
    }

  }

}
