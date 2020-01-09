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

package org.apache.spark.shuffle.sort.io;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.spark.MapOutputTracker;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.BlockStoreShuffleReader;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShuffleMetadata;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.ShuffleBlockFetcherIterator;
import org.apache.spark.util.CompletionIterator;

// Gah.
import static org.apache.spark.internal.config.package$.MODULE$;

public class LocalDiskShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;
  private SerializerManager serializerManager;
  private IndexShuffleBlockResolver blockResolver;

  public LocalDiskShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @VisibleForTesting
  public LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.blockResolver = blockResolver;
  }

  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    serializerManager = SparkEnv.get().serializerManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return new LocalDiskShuffleMapOutputWriter(
        shuffleId, mapTaskId, numPartitions, blockManager.shuffleServerId(), blockResolver,
        sparkConf);
  }

  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
  }

  @Override
  public Iterator<InputStream> readShuffle(
      int shuffleId,
      int startPartition,
      int endPartition,
      Optional<ShuffleMetadata> metadata,
      Optional<Integer> mapIndex) throws IOException {
    // XXX: This is ugly. Perhaps should be in Scala.

    LocalDiskShuffleMetadata localMetadata = (LocalDiskShuffleMetadata) metadata.get();

    scala.collection.Iterator<InputStream> blockIter = new ShuffleBlockFetcherIterator(
      TaskContext.get(),
      blockManager.blockStoreClient(),
      blockManager,
      MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition,
        localMetadata.statuses, scala.Option.apply(mapIndex.orElse(null))),
      // XXX. this should come from the shuffle dep, but that's not available here.
      serializerManager::wrapStream,
      ((long) sparkConf.get(MODULE$.REDUCER_MAX_SIZE_IN_FLIGHT()) * 1024 * 1024),
      ((int) sparkConf.get(MODULE$.REDUCER_MAX_REQS_IN_FLIGHT())),
      ((int) sparkConf.get(MODULE$.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS())),
      ((long) sparkConf.get(MODULE$.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM())),
      ((boolean) sparkConf.get(MODULE$.SHUFFLE_DETECT_CORRUPT())),
      ((boolean) sparkConf.get(MODULE$.SHUFFLE_DETECT_CORRUPT_MEMORY())),
      TaskContext.get().taskMetrics().createTempShuffleReadMetrics(),
      false /* see BlockStoreShuffleReader; need to implement this somehow */);

    return new Iterator<InputStream>() {

      @Override
      public boolean hasNext() {
        return blockIter.hasNext();
      }

      @Override
      public InputStream next() {
        return blockIter.next();
      }

    };
  }
}
