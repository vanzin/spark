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

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.io.output.{ByteArrayOutputStream => ApacheByteArrayOutputStream}

import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.shuffle.api.{MapOutputMetadata, ShuffleMetadata, ShuffleOutputTracker}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util._

private[spark] sealed trait MapOutputTrackerMessage
private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage
private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)

/** RpcEndpoint class for MapOutputTrackerMaster */
private[spark] class MapOutputTrackerMasterEndpoint(
    override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      tracker.post(new GetMapOutputMessage(shuffleId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
 * Class that keeps track of the location of the map output of a stage. This is abstract because the
 * driver and executor have different versions of the MapOutputTracker. In principle the driver-
 * and executor-side classes don't need to share a common base class; the current shared base class
 * is maintained primarily for backwards-compatibility in order to avoid having to update existing
 * test code.
*/
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
  /** Set to the MapOutputTrackerMasterEndpoint living on the driver. */
  var trackerEndpoint: RpcEndpointRef = _

  /**
   * The driver-side counter is incremented every time that a map output is lost. This value is sent
   * to executors as part of tasks, where executors compare the new epoch number to the highest
   * epoch number that they received in the past. If the new epoch number is higher then executors
   * will clear their local caches of map output statuses and will re-fetch (possibly updated)
   * statuses from the driver.
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /**
   * Send a message to the trackerEndpoint and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. */
  protected def sendTracker(message: Any): Unit = {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }
  /**
   * Deletes map output status information for the specified shuffle stage.
   */
  def unregisterShuffle(shuffleId: Int): Unit

  def getShuffleMetadata(shuffleId: Int): Option[ShuffleMetadata]

  def stop(): Unit = {}
}

/**
 * Driver-side class that keeps track of the location of the map output of a stage.
 *
 * The DAGScheduler uses this class to (de)register map output statuses and to look up statistics
 * for performing locality-aware reduce task scheduling.
 *
 * ShuffleMapStage uses this class for tracking available / missing outputs in order to determine
 * which tasks need to be run.
 */
private[spark] class MapOutputTrackerMaster(
    conf: SparkConf,
    private[spark] val broadcastManager: BroadcastManager,
    private[spark] val isLocal: Boolean)
  extends MapOutputTracker(conf) {

  // XXX: should really be a constructor argument, but initialization is a web of interdependencies
  // making that pretty hard.
  var outputTracker: Option[ShuffleOutputTracker] = None

  // The size at which we use Broadcast to send the map output statuses to the executors
  private val minSizeForBroadcast = conf.get(SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST).toInt

  /** Whether to compute locality preferences for reduce tasks */
  private val shuffleLocalityEnabled = conf.get(SHUFFLE_REDUCE_LOCALITY_ENABLE)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // requests for map output statuses
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]

  private val knownShuffles = new ConcurrentHashMap[Int, ShuffleState]()

  // Thread pool used for handling map output status requests. This is a separate thread pool
  // to ensure we don't block the normal dispatcher threads.
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.get(SHUFFLE_MAPOUTPUT_DISPATCHER_NUM_THREADS)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  // Make sure that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"${SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST.key} ($minSizeForBroadcast bytes) " +
      s"must be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an " +
      "rpc message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  def post(message: GetMapOutputMessage): Unit = {
    mapOutputRequests.offer(message)
  }

  private def serializedStatus(shuffleId: Int): Array[Byte] = {
    val state = shuffleState(shuffleId)
    var data = state.withReadLock(_.serialized)

    if (data == null) {
      val (newData, bcast, statusEpoch) = state.withReadLock { _ =>
        val metadata = outputTracker.flatMap { ot =>
          Option(ot.shuffleMetadata(shuffleId).orElse(null))
        }
        val (_data, _bcast) = MapOutputTracker.serializeMapStatuses(metadata, broadcastManager,
          isLocal, minSizeForBroadcast, conf)
        (_data, _bcast, getEpoch)
      }

      data = state.withWriteLock { _ =>
        if (state.cache(newData, bcast, statusEpoch)) {
          newData
        } else {
          if (bcast != null) {
            broadcastManager.unbroadcast(bcast.id, true, false)
          }
          state.serialized
        }
      }
    }

    data
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputRequests.take()
             if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            val context = data.context
            val shuffleId = data.shuffleId
            val hostPort = context.senderAddress.hostPort
            logDebug("Handling request to send map output locations for shuffle " + shuffleId +
              " to " + hostPort)
            context.reply(serializedStatus(shuffleId))
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new GetMapOutputMessage(-99, null)

  private def shuffleState(shuffleId: Int): ShuffleState = {
    val state = knownShuffles.get(shuffleId)
    assert(state != null, s"Cannot find shuffle $shuffleId.")
    state
  }

  def registerShuffle(shuffleId: Int, numMaps: Int): Unit = {
    val newState = new ShuffleState()
    val previous = knownShuffles.putIfAbsent(shuffleId, newState)
    assert(previous == null, s"Duplicate shuffle registration for $shuffleId.")
    outputTracker.foreach(_.registerShuffle(shuffleId, numMaps))
  }

  def registerMapOutput(shuffleId: Int, mapIndex: Int, status: MapOutputMetadata): Unit = {
    outputTracker.foreach { ot =>
      shuffleState(shuffleId).withWriteLock { state =>
        if (ot.registerOutput(shuffleId, mapIndex, status)) {
          state.clearCache()
        }
      }
    }
  }

  /** Unregister map output information of the given shuffle, mapper and block manager */
  def unregisterMapOutput(shuffleId: Int, mapIndex: Int, bmAddress: BlockManagerId): Unit = {
    logError(s"Need to implement proper fetch failure support.")
  }

  /** Unregister all map output information of the given shuffle. */
  def unregisterAllMapOutput(shuffleId: Int): Unit = {
    outputTracker.foreach { ot =>
      val modified = shuffleState(shuffleId).withWriteLock { state =>
        if (ot.invalidateShuffle(shuffleId)) {
          state.clearCache()
          true
        } else {
          false
        }
      }
      if (modified) {
        incrementEpoch()
      }
    }
  }

  /** Unregister shuffle data */
  def unregisterShuffle(shuffleId: Int): Unit = {
    val state = knownShuffles.remove(shuffleId)
    assert(state != null, s"Trying to remove unknown shuffle $shuffleId.")
    outputTracker.foreach { ot =>
      state.withWriteLock { _ =>
        ot.unregisterShuffle(shuffleId)
      }
    }
  }

  /**
   * Removes all shuffle outputs associated with this host. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists).
   */
  def removeOutputsOnHost(host: String): Unit = {
    logError(s"Need to implement proper fetch failure support.")
    incrementEpoch()
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
  def removeOutputsOnExecutor(execId: String): Unit = {
    logError(s"Need to implement proper fetch failure support.")
    incrementEpoch()
  }

  /** Check if the given shuffle is being tracked */
  def containsShuffle(shuffleId: Int): Boolean = knownShuffles.containsKey(shuffleId)

  def getNumAvailableOutputs(shuffleId: Int): Int = {
    outputTracker.map { ot =>
      shuffleState(shuffleId).withReadLock { _ => ot.numAvailableOutputs(shuffleId) }
    }.getOrElse(0)
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed), or None
   * if the MapOutputTrackerMaster doesn't know about this shuffle.
   */
  def findMissingPartitions(shuffleId: Int): Option[Seq[Int]] = {
    // XXX: previous code ignored unknown shuffles here, so do the same.
    Option(knownShuffles.get(shuffleId)).flatMap { state =>
      outputTracker.map { ot =>
        state.withReadLock { _ => ot.findMissingPartitions(shuffleId) }.toSeq
      }
    }
  }

  /**
   * Grouped function of Range, this is to avoid traverse of all elements of Range using
   * IterableLike's grouped function.
   */
  def rangeGrouped(range: Range, size: Int): Seq[Range] = {
    val start = range.start
    val step = range.step
    val end = range.end
    for (i <- start.until(end, size * step)) yield {
      i.until(i + size * step, step)
    }
  }

  /**
   * To equally divide n elements into m buckets, basically each bucket should have n/m elements,
   * for the remaining n%m elements, add one more element to the first n%m buckets each.
   */
  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Seq[Int]] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    if (elementsPerBucket == 0) {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1)
    } else {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1) ++
        rangeGrouped(splitPoint.until(numElements), elementsPerBucket)
    }
  }

  /**
   * Return statistics about all of the outputs for a given shuffle.
   */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    new MapOutputStatistics(dep.shuffleId, new Array[Long](dep.partitioner.numPartitions))
    /*
    TODO: no idea.
    shuffleStatuses(dep.shuffleId).withMapStatuses { statuses =>
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      val parallelAggThreshold = conf.get(
        SHUFFLE_MAP_OUTPUT_PARALLEL_AGGREGATION_THRESHOLD)
      val parallelism = math.min(
        Runtime.getRuntime.availableProcessors(),
        statuses.length.toLong * totalSizes.length / parallelAggThreshold + 1).toInt
      if (parallelism <= 1) {
        for (s <- statuses) {
          for (i <- 0 until totalSizes.length) {
            totalSizes(i) += s.getSizeForBlock(i)
          }
        }
      } else {
        val threadPool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "map-output-aggregate")
        try {
          implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
          val mapStatusSubmitTasks = equallyDivide(totalSizes.length, parallelism).map {
            reduceIds => Future {
              for (s <- statuses; i <- reduceIds) {
                totalSizes(i) += s.getSizeForBlock(i)
              }
            }
          }
          ThreadUtils.awaitResult(Future.sequence(mapStatusSubmitTasks), Duration.Inf)
        } finally {
          threadPool.shutdown()
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes)
    }
    */
  }

  /**
   * Return the preferred hosts on which to run the given map output partition in a given shuffle,
   * i.e. the nodes that the most outputs for that partition are on.
   *
   * @param dep shuffle dependency object
   * @param partitionId map output partition that we want to read
   * @return a sequence of host names
   */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
      : Seq[String] = {
    outputTracker.toSeq.flatMap { ot =>
      val sstate = knownShuffles.get(dep.shuffleId)
      if (sstate != null) {
        sstate.withReadLock { _ =>
          ot.preferredLocations(dep.shuffleId, partitionId, false).asScala
        }
      } else {
        Nil
      }
    }
  }

  /**
   * Return the location where the Mapper ran. The locations each includes both a host and an
   * executor id on that host.
   *
   * @param dep shuffle dependency object
   * @param mapId the map id
   * @return a sequence of locations where task runs.
   */
  def getMapLocation(dep: ShuffleDependency[_, _, _], mapId: Int): Seq[String] =
  {
    outputTracker.toSeq.flatMap { ot =>
      val sstate = knownShuffles.get(dep.shuffleId)
      if (sstate != null) {
        sstate.withReadLock { _ =>
          ot.preferredLocations(dep.shuffleId, mapId, true).asScala
        }
      } else {
        Nil
      }
    }
  }

  def incrementEpoch(): Unit = {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  override def stop(): Unit = {
    mapOutputRequests.offer(PoisonPill)
    threadpool.shutdown()
    sendTracker(StopMapOutputTracker)
    trackerEndpoint = null
  }

  override def getShuffleMetadata(shuffleId: Int): Option[ShuffleMetadata] = {
    outputTracker match {
      case Some(ot) => Option(ot.shuffleMetadata(shuffleId).orElse(null))
      case _ => None
    }
  }
}

/**
 * Executor-side client for fetching map output info from the driver's MapOutputTrackerMaster.
 * Note that this is not used in local-mode; instead, local-mode Executors access the
 * MapOutputTrackerMaster directly (which is possible because the master and worker share a common
 * superclass).
 */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {

  val mapStatuses = new ConcurrentHashMap[Int, Option[ShuffleMetadata]]()

  /**
   * A [[KeyLock]] whose key is a shuffle id to ensure there is only one thread fetching
   * the same shuffle block.
   */
  private val fetchingLock = new KeyLock[Int]

  /**
   * Get or fetch the array of MapStatuses for a given shuffle ID. NOTE: clients MUST synchronize
   * on this array when reading it, because on the driver, we may be changing it in place.
   *
   * (It would be nice to remove this restriction in the future.)
   */
  private def getStatuses(shuffleId: Int, conf: SparkConf): Option[ShuffleMetadata] = {
    val statuses = mapStatuses.get(shuffleId)
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTimeNs = System.nanoTime()
      fetchingLock.withLock(shuffleId) {
        var fetchedStatuses = mapStatuses.get(shuffleId)
        if (fetchedStatuses == null) {
          logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes, conf)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        }
        logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
          s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms")
        fetchedStatuses
      }
    } else {
      statuses
    }
  }


  /** Unregister shuffle data. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
  }

  /**
   * Called from executors to update the epoch number, potentially clearing old outputs
   * because of a fetch failure. Each executor task calls this with the latest epoch
   * number on the driver at the time it was created.
   */
  def updateEpoch(newEpoch: Long): Unit = {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  override def getShuffleMetadata(shuffleId: Int): Option[ShuffleMetadata] = {
    getStatuses(shuffleId, conf)
  }
}

private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using Zstd. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(
      statuses: Option[ShuffleMetadata],
      broadcastManager: BroadcastManager,
      isLocal: Boolean,
      minBroadcastSize: Int,
      conf: SparkConf): (Array[Byte], Broadcast[Array[Byte]]) = {
    // Using `org.apache.commons.io.output.ByteArrayOutputStream` instead of the standard one
    // This implementation doesn't reallocate the whole memory block but allocates
    // additional buffers. This way no buffers need to be garbage collected and
    // the contents don't have to be copied to the new buffer.
    val out = new ApacheByteArrayOutputStream()
    out.write(DIRECT)
    val codec = CompressionCodec.createCodec(conf, conf.get(MAP_STATUS_COMPRESSION_CODEC))
    val objOut = new ObjectOutputStream(codec.compressedOutputStream(out))
    Utils.tryWithSafeFinally {
      objOut.writeObject(statuses)
    } {
      objOut.close()
    }
    val arr = out.toByteArray
    if (arr.length >= minBroadcastSize) {
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // toByteArray creates copy, so we can reuse out
      out.reset()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(codec.compressedOutputStream(out))
      Utils.tryWithSafeFinally {
        oos.writeObject(bcast)
      } {
        oos.close()
      }
      val outArr = out.toByteArray
      logInfo("Broadcast mapstatuses size = " + outArr.length + ", actual size = " + arr.length)
      (outArr, bcast)
    } else {
      (arr, null)
    }
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte], conf: SparkConf): Option[ShuffleMetadata] = {
    assert (bytes.length > 0)

    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val codec = CompressionCodec.createCodec(conf, conf.get(MAP_STATUS_COMPRESSION_CODEC))
      // The ZStd codec is wrapped in a `BufferedInputStream` which avoids overhead excessive
      // of JNI call while trying to decompress small amount of data for each element
      // of `MapStatuses`
      val objIn = new ObjectInputStream(codec.compressedInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[Option[ShuffleMetadata]]
      case BROADCAST =>
        // deserialize the Broadcast, pull .value array out of it, and then deserialize that
        val bcast = deserializeObject(bytes, 1, bytes.length - 1).
          asInstanceOf[Broadcast[Array[Byte]]]
        logInfo("Broadcast mapstatuses size = " + bytes.length +
          ", actual size = " + bcast.value.length)
        // Important - ignore the DIRECT tag ! Start from offset 1
        deserializeObject(bcast.value, 1, bcast.value.length - 1)
          .asInstanceOf[Option[ShuffleMetadata]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
   * Given an array of map statuses and a range of map output partitions, returns a sequence that,
   * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
   * stored at that block manager.
   * Note that empty blocks are filtered in the result.
   *
   * If any of the statuses is null (indicating a missing location due to a failed mapper),
   * throws a FetchFailedException.
   *
   * @param shuffleId Identifier for the shuffle
   * @param startPartition Start of map output partition ID range (included in range)
   * @param endPartition End of map output partition ID range (excluded from range)
   * @param statuses List of map statuses, indexed by map partition index.
   * @param mapIndex When specified, only shuffle blocks from this mapper will be processed.
   * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
   *         and the second item is a sequence of (shuffle block id, shuffle block size, map index)
   *         tuples describing the shuffle blocks that are stored at that block manager.
   */
  def convertMapStatuses(
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      statuses: Array[MapStatus],
      mapIndex : Option[Int] = None): Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    assert (statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ListBuffer[(BlockId, Long, Int)]]
    val iter = statuses.iterator.zipWithIndex
    for ((status, mapIndex) <- mapIndex.map(index => iter.filter(_._2 == index)).getOrElse(iter)) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
      } else {
        for (part <- startPartition until endPartition) {
          val size = status.getSizeForBlock(part)
          if (size != 0) {
            splitsByAddress.getOrElseUpdate(status.location, ListBuffer()) +=
              ((ShuffleBlockId(shuffleId, status.mapId, part), size, mapIndex))
          }
        }
      }
    }

    splitsByAddress.iterator
  }
}

/** Wrapper around shuffle state that provides thread-safety. */
private class ShuffleState {

  private val lock = new ReentrantReadWriteLock()

  private var _serialized: Array[Byte] = null
  private var broadcast: Broadcast[Array[Byte]] = null
  private var epoch: Long = -1L

  def withReadLock[T](fn: ShuffleState => T): T = {
    lock.readLock().lock()
    try {
      fn(this)
    } finally {
      lock.readLock().unlock()
    }
  }

  def withWriteLock[T](fn: ShuffleState => T): T = {
    lock.writeLock().lock()
    try {
      fn(this)
    } finally {
      lock.writeLock().unlock()
    }
  }

  def clearCache(): Unit = {
    _serialized = null
    broadcast = null
    epoch = -1L
  }

  def cache(data: Array[Byte], bcast: Broadcast[Array[Byte]], callerEpoch: Long): Boolean = {
    if (callerEpoch > epoch) {
      _serialized = data
      broadcast = bcast
      epoch = callerEpoch
      true
    } else {
      false
    }
  }

  def serialized: Array[Byte] = _serialized
}
