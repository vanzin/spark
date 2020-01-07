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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;

import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.api.FetchFailedException;
import org.apache.spark.shuffle.api.MapOutputMetadata;
import org.apache.spark.shuffle.api.ShuffleMetadata;
import org.apache.spark.shuffle.api.ShuffleOutputTracker;
import org.apache.spark.storage.BlockManagerId;

class LocalDiskShuffleMetadata implements ShuffleMetadata {

  final MapStatus[] statuses;

  LocalDiskShuffleMetadata(MapStatus[] statuses) {
    this.statuses = statuses;
  }

}

class ShuffleStatus {

  /**
   * MapStatus for each partition. The index of the array is the map partition id.
   * Each value in the array is the MapStatus for a partition, or null if the partition
   * is not available. Even though in theory a task may run multiple times (due to speculation,
   * stage retries, etc.), in practice the likelihood of a map output being available at multiple
   * locations is so small that we choose to ignore that case and store only a single location
   * for each output.
   */
  private final MapStatus[] statuses;

  /**
   * Counter tracking the number of partitions that have output. This is a performance optimization
   * to avoid having to count the number of non-null entries in the `mapStatuses` array and should
   * be equivalent to`mapStatuses.count(_ ne null)`.
   */
  private int numAvailableOutputs;

  ShuffleStatus(int numMaps) {
    statuses = new MapStatus[numMaps];
  }

  /**
   * Register a map output. If there is already a registered location for the map output then it
   * is ignored.
   *
   * @return Whether the status was added to the internal list.
   */
  boolean addMapOutput(int mapIndex, MapStatus status) {
    if (statuses[mapIndex] == null) {
      numAvailableOutputs += 1;
      statuses[mapIndex] = status;
      return true;
    }
    return false;
  }

  /**
   * Remove the map output which was served by the specified block manager.
   * This is a no-op if there is no registered map output or if the registered output is from a
   * different block manager.
   *
   * @return Whether the status was removed from the internal list.
   */
  boolean removeMapOutput(int mapIndex, BlockManagerId bmAddress) {
    if (statuses[mapIndex] != null && statuses[mapIndex].location().equals(bmAddress)) {
      numAvailableOutputs -= 1;
      statuses[mapIndex] = null;
      return true;
    }
    return false;
  }

  /**
   * Removes all shuffle outputs associated with this host. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists).
   *
   * @return Whether at least one map status was removed.
   */
  boolean removeOutputsOnHost(String host) {
    return removeOutputsByFilter(x -> x.host().equals(host));
  }

  /**
   * Removes all map outputs associated with the specified executor. Note that this will also
   * remove outputs which are served by an external shuffle server (if one exists), as they are
   * still registered with that execId.
   *
   * @return Whether at least one map status was removed.
   */
  boolean removeOutputsOnExecutor(String execId) {
    return removeOutputsByFilter(x -> x.executorId().equals(execId));
  }

  /**
   * Removes all shuffle outputs which satisfies the filter. Note that this will also
   * remove outputs which are served by an external shuffle server (if one exists).
   */
  boolean removeOutputsByFilter(Predicate<BlockManagerId> filter) {
    boolean modified = false;
    for (int i = 0; i < statuses.length; i ++) {
      if (statuses[i] != null && filter.test(statuses[i].location())) {
        numAvailableOutputs -= 1;
        statuses[i] = null;
        modified = true;
      }
    }
    return modified;
  }

  boolean invalidate() {
    if (numAvailableOutputs > 0) {
      for (int i = 0; i < statuses.length; i++) {
        statuses[i] = null;
      }
      numAvailableOutputs = 0;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Number of partitions that have shuffle outputs.
   */
  int numAvailableOutputs() {
    return numAvailableOutputs;
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   */
  int[] findMissingPartitions() {
    int[] missing = new int[statuses.length - numAvailableOutputs];
    int midx = 0;
    for (int i = 0; i < statuses.length; i++) {
      if (statuses[i] == null) {
        Preconditions.checkState(midx < missing.length,
          "Expected %s missing, but found more.", missing.length);
        missing[midx] = i;
        midx++;
      }
    }
    return missing;
  }

  MapStatus mapStatus(int mapperIndex) {
    return statuses[mapperIndex];
  }

  int mapperCount() {
    return statuses.length;
  }

  LocalDiskShuffleMetadata toMetadata() {
    return new LocalDiskShuffleMetadata(statuses);
  }
}

class LocalDiskOutputTracker implements ShuffleOutputTracker {

  private final Map<Integer, ShuffleStatus> shuffles = new HashMap<>();

  @Override
  public void registerShuffle(int shuffleId, int numMaps) {
    Preconditions.checkState(!shuffles.containsKey(shuffleId), "Shuffle %s already registered.",
      shuffleId);
    shuffles.put(shuffleId, new ShuffleStatus(numMaps));
  }

  @Override
  public boolean registerOutput(int shuffleId, int mapIndex, MapOutputMetadata status) {
    return shuffle(shuffleId).addMapOutput(mapIndex, (MapStatus) status);
  }

  @Override
  public boolean handleFetchFailure(FetchFailedException e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean invalidateShuffle(int shuffleId) {
    return shuffle(shuffleId).invalidate();
  }

  @Override
  public void unregisterShuffle(int shuffleId) {
    shuffles.remove(shuffleId);
  }

  @Override
  public int[] findMissingPartitions(int shuffleId) {
    return shuffle(shuffleId).findMissingPartitions();
  }

  @Override
  public int numAvailableOutputs(int shuffleId) {
    ShuffleStatus shuffle = shuffles.get(shuffleId);
    return shuffle != null ? shuffle.numAvailableOutputs() : 0;
  }

  @Override
  public Optional<ShuffleMetadata> shuffleMetadata(int shuffleId) {
    ShuffleStatus shuffle = shuffles.get(shuffleId);
    return Optional.ofNullable(shuffle != null ? shuffle.toMetadata() : null);
  }

  @Override
  public List<String> preferredLocations(
      int shuffleId,
      int mapperOrPartitionId,
      boolean isMapper) {
    ShuffleStatus shuffle = shuffles.get(shuffleId);
    if (shuffle == null) {
      return Collections.emptyList();
    }

    if (isMapper) {
      if (mapperOrPartitionId > 0 && mapperOrPartitionId < shuffle.mapperCount()) {
        MapStatus m = shuffle.mapStatus(mapperOrPartitionId);
        return Arrays.asList(m.location().host());
      } else {
        return Collections.emptyList();
      }
    }

    // Fraction of total map output that must be at a location for it to considered as a preferred
    // location for a reduce task. Making this larger will focus on fewer locations where most data
    // can be read locally, but may lead to more delay in scheduling if those locations are busy.
    final double REDUCER_PREF_LOCS_FRACTION = 0.2;

    Map<BlockManagerId, Long> locs = new HashMap<>();
    long totalOutputSize = 0L;
    for (int mapIdx = 0; mapIdx < shuffle.mapperCount(); mapIdx++) {
      MapStatus status = shuffle.mapStatus(mapIdx);
      // status may be null here if we are called between registerShuffle, which creates an
      // array with null entries for each output, and registerMapOutputs, which populates it
      // with valid status entries. This is possible if one thread schedules a job which
      // depends on an RDD which is currently being computed by another thread.
      if (status != null) {
        long blockSize = status.getSizeForBlock(mapperOrPartitionId);
        if (blockSize > 0) {
          Long previous = locs.get(status.location());
          Long updated = previous != null ? previous + blockSize : blockSize;
          locs.put(status.location(), updated);
          totalOutputSize += blockSize;
        }
      }
    }

    List<String> topLocs = new ArrayList<>();
    for (Map.Entry<BlockManagerId, Long> e : locs.entrySet()) {
      if (1.0d * e.getValue() / totalOutputSize >= REDUCER_PREF_LOCS_FRACTION) {
        topLocs.add(e.getKey().host());
      }
    }
    return topLocs;
  }

  private ShuffleStatus shuffle(int shuffleId) {
    ShuffleStatus shuffle = shuffles.get(shuffleId);
    Preconditions.checkState(shuffle != null, "Shuffle %s not registered.", shuffleId);
    return shuffle;
  }
}
