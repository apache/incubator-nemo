package org.apache.nemo.runtime.master.resource;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSlotProperty;
import org.apache.nemo.runtime.common.MetricManagerWorker;
import org.apache.nemo.runtime.common.TaskStateManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.reef.driver.context.ActiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LambdaExecutorRepresenter implements ExecutorRepresenter {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutorRepresenter.class.getName());

  private final String executorId;
  private final ResourceSpecification resourceSpecification;
  private final Map<String, Task> runningComplyingTasks;
  private final Map<String, Task> runningNonComplyingTasks;
  private final Map<Task, Integer> runningTaskToAttempt;
  private final Set<Task> completeTasks;
  private final Set<Task> failedTasks;
  private final MessageSender<ControlMessage.Message> messageSender;
  private final ActiveContext activeContext;
  private final ExecutorService serializationExecutorService;
  private final String nodeName;

  private final AWSLambda client;
  private final String LambdaFunctionName;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final MetricManagerWorker metricMessageSender;

  /**
   * Creates a reference to the specified executor.
   *
   * @param executorId                   the executor id
   * @param resourceSpecification        specification for the executor
   * @param messageSender                provides communication context for this executor
   * @param activeContext                context on the corresponding REEF evaluator
   * @param serializationExecutorService provides threads for message serialization
   * @param nodeName                     physical name of the node where this executor resides
   */
  public LambdaExecutorRepresenter(final String executorId,
                                   final ResourceSpecification resourceSpecification,
                                   final MessageSender<ControlMessage.Message> messageSender,
                                   final ActiveContext activeContext,
                                   final ExecutorService serializationExecutorService,
                                   final String nodeName,
                                   final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                                   final MetricManagerWorker metricMessageSender) {
    this.executorId = executorId;
    // Add Lambda Resource type?
    this.resourceSpecification = resourceSpecification;

    this.messageSender = messageSender;
    this.runningComplyingTasks = new HashMap<>();
    this.runningNonComplyingTasks = new HashMap<>();
    this.runningTaskToAttempt = new HashMap<>();
    this.completeTasks = new HashSet<>();
    this.failedTasks = new HashSet<>();
    this.activeContext = activeContext;
    this.serializationExecutorService = serializationExecutorService;
    this.nodeName = nodeName;

    // Need a function reading user parameters such as lambda function name etc.
    Regions region = Regions.fromName("ap-northeast-1");
    this.client = AWSLambdaClientBuilder.standard().withRegion(region).build();
    this.LambdaFunctionName = "lambda-dev-executor";
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.metricMessageSender = metricMessageSender;
  }

  /**
   * Marks all Tasks which were running in this executor as failed.
   *
   * @return set of identifiers of tasks that were running in this executor.
   */
  @Override
  public Set<String> onExecutorFailed() {
    LOG.info("##### Lambda Executor Failed: " + this.toString());
    throw new UnsupportedOperationException();
  }

  /**
   * Marks the Task as running, and sends scheduling message to the executor.
   *
   * @param task the task to run
   */
  @Override
  public void onTaskScheduled(final Task task) {
    (task.getPropertyValue(ResourceSlotProperty.class).orElse(true)
      ? runningComplyingTasks : runningNonComplyingTasks).put(task.getTaskId(), task);
    runningTaskToAttempt.put(task, task.getAttemptIdx());

    // remove from failedTasks if this task failed before
    failedTasks.remove(task);

    final byte[] serialized = SerializationUtils.serialize(task);
    String json = "{\"d\":\"" + Base64.getEncoder().encodeToString(serialized) + "\"}";

    final TaskStateManager taskStateManager =
      new TaskStateManager(task, executorId, this.persistentConnectionToMasterMap, this.metricMessageSender);

    serializationExecutorService.submit(() -> {
      InvokeRequest request = new InvokeRequest()
        .withFunctionName(this.LambdaFunctionName)
        .withInvocationType("Event").withLogType("Tail").withClientContext("Lambda Executor " + task.getTaskId())
        .withPayload(json);
//    InvokeResult response = client.invoke(request);
      taskStateManager.onTaskStateChanged(TaskState.State.COMPLETE, Optional.empty(), Optional.empty());
      LOG.info("###### Request invoked! " + task.getTaskId());// + "\n" + response.toString());
    });
  }

  /**
   * Sends control message to the executor.
   *
   * @param message Message object to send
   */
  @Override
  public void sendControlMessage(final ControlMessage.Message message) {
    if(message.getType() == ControlMessage.MessageType.RequestMetricFlush) {
      messageSender.send(message);
      return ;
    }
    LOG.info("##### Lambda Executor control msg not supported " + message.getType());
    throw new UnsupportedOperationException();
  }

  /**
   * Marks the specified Task as completed.
   *
   * @param taskId id of the completed task
   */
  @Override
  public void onTaskExecutionComplete(final String taskId) {
    final Task completedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(completedTask);
    completeTasks.add(completedTask);
  }

  /**
   * Marks the specified Task as failed.
   *
   * @param taskId id of the Task
   */
  @Override
  public void onTaskExecutionFailed(final String taskId) {
    final Task failedTask = removeFromRunningTasks(taskId);
    runningTaskToAttempt.remove(failedTask);
    failedTasks.add(failedTask);
  }

  /**
   * @return how many Tasks can this executor simultaneously run
   */
  @Override
  public int getExecutorCapacity() {
    return resourceSpecification.getCapacity();
  }

  /**
   * @return the current snapshot of set of Tasks that are running in this executor.
   */
  @Override
  public Set<Task> getRunningTasks() {
    return Stream.concat(runningComplyingTasks.values().stream(),
      runningNonComplyingTasks.values().stream()).collect(Collectors.toSet());
  }

  /**
   * @return the number of running {@link Task}s.
   */
  @Override
  public int getNumOfRunningTasks() {
    return getNumOfComplyingRunningTasks() + getNumOfNonComplyingRunningTasks();
  }

  /**
   * @return the number of running {@link Task}s that complies to the executor slot restriction.
   */
  @Override
  public int getNumOfComplyingRunningTasks() {
    return runningComplyingTasks.size();
  }

  /**
   * @return the number of running {@link Task}s that does not comply to the executor slot restriction.
   */
  private int getNumOfNonComplyingRunningTasks() {
    return runningNonComplyingTasks.size();
  }

  /**
   * @return the executor id
   */
  @Override
  public String getExecutorId() {
    return executorId;
  }

  /**
   * @return the container type
   */
  @Override
  public String getContainerType() {
    return resourceSpecification.getContainerType();
  }

  /**
   * @return physical name of the node where this executor resides
   */
  @Override
  public String getNodeName() {
    return nodeName;
  }

  /**
   * Shuts down this executor.
   */
  @Override
  public void shutDown() {
    activeContext.close();
  }

  @Override
  public String toString() {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();
    node.put("executorId", executorId);
    node.put("runningTasks", getRunningTasks().toString());
    node.put("failedTasks", failedTasks.toString());
    return node.toString();
  }

  /**
   * Removes the specified {@link Task} from the map of running tasks.
   *
   * @param taskId id of the task to remove
   * @return the removed {@link Task}
   */
  private Task removeFromRunningTasks(final String taskId) {
    final Task task;
    if (runningComplyingTasks.containsKey(taskId)) {
      task = runningComplyingTasks.remove(taskId);
    } else if (runningNonComplyingTasks.containsKey(taskId)) {
      task = runningNonComplyingTasks.remove(taskId);
    } else {
      throw new RuntimeException(String.format("Task %s not found in its DefaultExecutorRepresenter", taskId));
    }
    return task;
  }
}
