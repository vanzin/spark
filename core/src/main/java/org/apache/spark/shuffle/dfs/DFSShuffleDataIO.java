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
import java.util.UUID;

import scala.Option;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.internal.config.Shuffle;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;

public class DFSShuffleDataIO implements ShuffleDataIO {

  private static final Logger LOG = LoggerFactory.getLogger(DFSShuffleDataIO.class);

  private Configuration conf;
  private FileSystem fs;
  private Path path;

  public DFSShuffleDataIO(SparkConf sparkConf) throws IOException {
    // If DFS_IO_PATH is not set, assume we're in the driver, and initialize it. This needs to
    // be done before initialize() so that it's set before any executors are alive.
    Option<String> pathOption = sparkConf.get(Shuffle.DFS_IO_PATH());
    if (pathOption.isEmpty()) {
      // TODO: The base path currently needs to be defined by the user. In YARN it should probably
      // be based on the staging dir unless configured otherwise.
      Option<String> basePathOption = sparkConf.get(Shuffle.DFS_IO_BASE_PATH());
      Preconditions.checkArgument(basePathOption.isDefined(),
        "Missing %s configuration.", Shuffle.DFS_IO_BASE_PATH().key());

      Path basePath = new Path(basePathOption.get());

      // TODO: this configuration should come from the driver somehow.
      Configuration conf = SparkHadoopUtil.get().newConfiguration(sparkConf);
      FileSystem fs = basePath.getFileSystem(conf);

      Path path;
      do {
        path = new Path(basePath, UUID.randomUUID().toString());
      } while (fs.exists(path));

      if (!fs.mkdirs(path)) {
        throw new IOException("Failed to create shuffle directory " + path);
      }

      sparkConf.set(Shuffle.DFS_IO_PATH(), path.toString());
      LOG.info("Shuffle data will be written to {}.", path.toString());
    }
  }

  @Override
  public void initialize() throws IOException {
    SparkConf sparkConf = SparkEnv.get().conf();
    Option<String> pathOption = sparkConf.get(Shuffle.DFS_IO_PATH());
    Preconditions.checkState(pathOption.isDefined(), "Shuffle directory has not been configured.");

    this.path = new Path(pathOption.get());

    // TODO: this configuration should come from the driver somehow.
    this.conf = SparkHadoopUtil.get().newConfiguration(sparkConf);
    this.fs = path.getFileSystem(conf);
  }

  @Override
  public ShuffleReadSupport readSupport() {
    return new DFSShuffleReadSupport(fs, path);
  }

  @Override
  public ShuffleWriteSupport writeSupport() throws IOException {
    return new DFSShuffleWriteSupport(conf, fs, path);
  }

  @Override
  public void shuffleCleaned(int shuffleId) throws Exception {
    Path shufflePath = new Path(path, String.format("shuffle-%d", shuffleId));
    fs.delete(shufflePath, true);
  }

  private static FileSystem getFileSystem(SparkConf sparkConf, Path path) throws IOException {
    // TODO: this configuration should come from the driver somehow.
    Configuration conf = SparkHadoopUtil.get().newConfiguration(sparkConf);
    return path.getFileSystem(conf);
  }

}
