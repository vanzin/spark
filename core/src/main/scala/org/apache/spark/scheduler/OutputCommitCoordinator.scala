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

package org.apache.spark.scheduler

import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private sealed trait OutputCommitCoordinationMessage extends Serializable

private case object StopCoordinator extends OutputCommitCoordinationMessage
private case class AskPermissionToCommitOutput(
    stage: Int,
    partition: Int,
    taskId: Long)

/**
 * Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"
 * policy.
 *
 * OutputCommitCoordinator is instantiated in both the drivers and executors. On executors, it is
 * configured with a reference to the driver's OutputCommitCoordinatorEndpoint, so requests to
 * commit output will be forwarded to the driver's OutputCommitCoordinator.
 *
 * This class was introduced in SPARK-4879; see that JIRA issue (and the associated pull requests)
 * for an extensive design discussion.
 */
private[spark] class OutputCommitCoordinator(conf: SparkConf, isDriver: Boolean) extends Logging {

  // Initialized by SparkEnv
  var coordinatorRef: Option[RpcEndpointRef] = None

  private val NO_AUTHORIZED_COMMITTER: Long = -1L

  private case class StageState(numPartitions: Int) {
    val authorizedCommitters = Array.fill[Long](numPartitions)(NO_AUTHORIZED_COMMITTER)
    val failures = mutable.Map[Int, mutable.Set[Long]]()
  }

  /**
   * Map from active stages's id => authorized task attempts for each partition id, which hold an
   * exclusive lock on committing task output for that partition, as well as any known failed
   * attempts in the stage.
   *
   * Entries are added to the top-level map when stages start and are removed they finish
   * (either successfully or unsuccessfully).
   *
   * Access to this map should be guarded by synchronizing on the OutputCommitCoordinator instance.
   */
  private val stageStates = mutable.Map[Int, StageState]()

  /**
   * Returns whether the OutputCommitCoordinator's internal data structures are all empty.
   */
  def isEmpty: Boolean = {
    stageStates.isEmpty
  }

  /**
   * Called by tasks to ask whether they can commit their output to HDFS.
   *
   * If a task attempt has been authorized to commit, then all other attempts to commit the same
   * task will be denied.  If the authorized task attempt fails (e.g. due to its executor being
   * lost), then a subsequent task attempt may be authorized to commit its output.
   *
   * @param stage the stage number
   * @param partition the partition number
   * @param taskId the task asking for permission to commit
   * @return true if this task is authorized to commit, false otherwise
   */
  def canCommit(
      stage: Int,
      partition: Int,
      taskId: Long): Boolean = {
    val msg = AskPermissionToCommitOutput(stage, partition, taskId)
    coordinatorRef match {
      case Some(endpointRef) =>
        ThreadUtils.awaitResult(endpointRef.ask[Boolean](msg),
          RpcUtils.askRpcTimeout(conf).duration)
      case None =>
        logError(
          "canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?")
        false
    }
  }

  /**
   * Called by the DAGScheduler when a stage starts. Initializes the stage's state if it hasn't
   * yet been initialized.
   *
   * @param stage the stage id.
   * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
   *                       the maximum possible value of `context.partitionId`).
   */
  private[scheduler] def stageStart(stage: Int, maxPartitionId: Int): Unit = synchronized {
    stageStates.get(stage) match {
      case Some(state) =>
        require(state.authorizedCommitters.length == maxPartitionId + 1)
        logInfo(s"Reusing state from previous attempt of stage $stage.")

      case _ =>
        stageStates(stage) = new StageState(maxPartitionId + 1)
    }
  }

  // Called by DAGScheduler
  private[scheduler] def stageEnd(stage: Int): Unit = synchronized {
    stageStates.remove(stage)
  }

  // Called by DAGScheduler
  private[scheduler] def taskCompleted(
      stage: Int,
      partition: Int,
      taskId: Long,
      reason: TaskEndReason): Unit = synchronized {
    val stageState = stageStates.getOrElse(stage, {
      logDebug(s"Ignoring task completion for completed stage")
      return
    })
    reason match {
      case Success =>
      // The task output has been committed successfully
      case _: TaskCommitDenied =>
        logInfo(s"Task was denied committing, stage: $stage, partition: $partition, task: $taskId")
      case _ =>
        // Mark the attempt as failed to blacklist from future commit protocol
        stageState.failures.getOrElseUpdate(partition, mutable.Set()) += taskId
        if (stageState.authorizedCommitters(partition) == taskId) {
          logDebug(s"Authorized committer (taskId=$taskId, stage=$stage, " +
            s"partition=$partition) failed; clearing lock")
          stageState.authorizedCommitters(partition) = NO_AUTHORIZED_COMMITTER
        }
    }
  }

  def stop(): Unit = synchronized {
    if (isDriver) {
      coordinatorRef.foreach(_ send StopCoordinator)
      coordinatorRef = None
      stageStates.clear()
    }
  }

  // Marked private[scheduler] instead of private so this can be mocked in tests
  private[scheduler] def handleAskPermissionToCommit(
      stage: Int,
      partition: Int,
      taskId: Long): Boolean = synchronized {
    stageStates.get(stage) match {
      case Some(state) if attemptFailed(state, partition, taskId) =>
        logInfo(s"Commit denied for stage=$stage, partition=$partition: " +
          s"task $taskId already marked as failed.")
        false
      case Some(state) =>
        val existing = state.authorizedCommitters(partition)
        if (existing == NO_AUTHORIZED_COMMITTER) {
          logDebug(s"Commit allowed for stage=$stage, partition=$partition, task $taskId")
          state.authorizedCommitters(partition) = taskId
          true
        } else {
          logDebug(s"Commit denied for stage=$stage, partition=$partition: " +
            s"already committed by $existing")
          false
        }
      case None =>
        logDebug(s"Commit denied for stage=$stage, partition=$partition: stage already completed.")
        false
    }
  }

  private def attemptFailed(
      stageState: StageState,
      partition: Int,
      taskId: Long): Boolean = synchronized {
    stageState.failures.get(partition).exists(_.contains(taskId))
  }
}

private[spark] object OutputCommitCoordinator {

  // This endpoint is used only for RPC
  private[spark] class OutputCommitCoordinatorEndpoint(
      override val rpcEnv: RpcEnv, outputCommitCoordinator: OutputCommitCoordinator)
    extends RpcEndpoint with Logging {

    logDebug("init") // force eager creation of logger

    override def receive: PartialFunction[Any, Unit] = {
      case StopCoordinator =>
        logInfo("OutputCommitCoordinator stopped!")
        stop()
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case AskPermissionToCommitOutput(stage, partition, taskId) =>
        context.reply(
          outputCommitCoordinator.handleAskPermissionToCommit(stage, partition, taskId))
    }
  }
}
