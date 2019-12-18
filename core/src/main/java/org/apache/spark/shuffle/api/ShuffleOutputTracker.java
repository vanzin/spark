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

package org.apache.spark.shuffle.api;

import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * An interface for building shuffle output tracking support in the driver. Spark handles
 * synchronizing the data between driver and executors, so that shuffle metadata is available
 * when reduce tasks need to read shuffle files.
 */
@Private
public interface ShuffleOutputTracker {

  // Called for each shuffle (from DAGScheduler).
  void registerShuffle(int shuffleId, int numMaps);

  // Called for each task (from DAGScheduler)
  boolean registerOutput(int shuffleId, int mapIndex, MapOutputMetadata metadata);

  // Called by DAGScheduler when fetch failure happens.
  boolean handleFetchFailure(FetchFailedException e);

  // Called by DAGScheduler when all output for a given shuffle ID needs to be invalidated, e.g.
  // when a barrier stage task fails, or when a non-deterministic shuffle map stage fails.
  boolean invalidateShuffle(int shuffleId);

  // Stop tracking the given shuffle.
  void unregisterShuffle(int shuffleId);

  // Called by DAGScheduler when re-submitting a stage for recomputation.
  int[] findMissingPartitions(int shuffleId);

  // TODO: check if this is really necessary (used in ShufleMapStage.scala).
  int numAvailableOutputs(int shuffleId);

  // An opaque object provided to executors' readShuffle() calls containing the plugin-specific
  // metadata for a specific shuffle.
  Optional<ShuffleMetadata> shuffleMetadata(int shuffleId);

}
