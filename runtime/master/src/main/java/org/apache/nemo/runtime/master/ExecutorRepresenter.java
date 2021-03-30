package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.common.comm.ControlMessage;

import java.util.*;

public interface ExecutorRepresenter {


  /**
   * Marks all Tasks which were running in this executor as failed.
   *
   * @return set of identifiers of tasks that were running in this executor.
   */
  Set<String> onExecutorFailed();

  /**
   * Marks the Task as running, and sends scheduling message to the executor.
   * @param task the task to run
   */
  void onTaskScheduled(final Task task);

  /**
   * Sends control message to the executor.
   * @param message Message object to send
   */
  void sendControlMessage(final ControlMessage.Message message);

  /**
   * Marks the specified Task as completed.
   * @param taskId id of the completed task
   */
  void onTaskExecutionComplete(final String taskId);

  /**
   * Marks the specified Task as completed.
   * @param taskId id of the completed task
   */
  void onTaskExecutionStop(final String taskId);

  /**
   * Marks the specified Task as failed.
   * @param taskId id of the Task
   */
  void onTaskExecutionFailed(final String taskId);

  /**
   * @return how many Tasks can this executor simultaneously run
   */
  int getExecutorCapacity();

  /**
   * @return the current snapshot of set of Tasks that are running in this executor.
   */
  Set<Task> getRunningTasks();

  /**
   * @return the number of running {@link Task}s.
   */
  int getNumOfRunningTasks();

  /**
   * @return the number of running {@link Task}s that complies to the executor slot restriction.
   */
  int getNumOfComplyingRunningTasks();

  /**
   * @return the number of running {@link Task}s that does not comply to the executor slot restriction.
   */
  int getNumOfNonComplyingRunningTasks();

  /**
   * @return the executor id
   */
  String getExecutorId();

  /**
   * @return the container type
   */
  String getContainerType();

  /**
   * @return physical name of the node where this executor resides
   */
  String getNodeName();

  /**
   * Shuts down this executor.
   */
  void shutDown();
}