package edu.snu.vortex.runtime.driver;

import edu.snu.vortex.runtime.VortexMessage;
import edu.snu.vortex.runtime.evaluator.VortexExecutor;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

@Unit
public final class VortexDriver {

  private final EvaluatorRequestor evaluatorRequestor;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;
  private final VortexMaster vortexMaster;

  private final int numEvaluators;
  private final int evaluatorMem;
  private final int evaluatorCore;

  private final AtomicInteger numAllocatedEvaluators;
  private final AtomicInteger numRunningTasks;


  @Inject
  private VortexDriver(
      final EvaluatorRequestor evaluatorRequestor,
      final NameServer nameServer,
      final LocalAddressProvider localAddressProvider,
      final VortexMaster vortexMaster,
      @Parameter(Parameters.EvaluatorNum.class) final int numEvaluators,
      @Parameter(Parameters.EvaluatorMem.class) final int evaluatorMem,
      @Parameter(Parameters.EvaluatorCore.class) final int evaluatorCore) {
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.vortexMaster = vortexMaster;

    this.numEvaluators = numEvaluators;
    this.evaluatorMem = evaluatorMem;
    this.evaluatorCore = evaluatorCore;

    this.numAllocatedEvaluators = new AtomicInteger();
    this.numRunningTasks = new AtomicInteger();
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      final EvaluatorRequest request = new EvaluatorRequest.Builder<>()
          .setMemory(evaluatorMem)
          .setNumberOfCores(evaluatorCore)
          .setNumber(numEvaluators)
          .build();
      evaluatorRequestor.submit(request);
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      if (numAllocatedEvaluators.incrementAndGet() < numEvaluators) {
        launchExecutor(allocatedEvaluator);
      } else {
        throw new RuntimeException("Too many evaluators " + numAllocatedEvaluators.get());
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {

    @Override
    public void onNext(final RunningTask runningTask) {
      if (numRunningTasks.incrementAndGet() == numEvaluators) {
        vortexMaster.launchJob();
      }
    }
  }

  public final class TaskMessageHandler implements EventHandler<TaskMessage> {

    @Override
    public void onNext(final TaskMessage taskMessage) {
      final byte[] message = taskMessage.get();
      if (message != null) {
        vortexMaster.onExecutorMessage((VortexMessage) SerializationUtils.deserialize(message));
      }
    }
  }

  private void launchExecutor(final AllocatedEvaluator allocatedEvaluator) {
    final String executorId = allocatedEvaluator.getId() + "_vortex_executor";
    final Configuration contextConfiguration = Configurations.merge(
        ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, executorId + "_CONTEXT")
    //        .set(ContextConfiguration.ON_CONTEXT_STOP, VortexContextStopHandler.class)
            .build(),
        getNameResolverServiceConfiguration());
    allocatedEvaluator.submitContextAndTask(contextConfiguration, getExecutorConfiguration(executorId));
  }

  private Configuration getExecutorConfiguration(final String executorId) {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, executorId)
        .set(TaskConfiguration.TASK, VortexExecutor.class)
        .set(TaskConfiguration.ON_SEND_MESSAGE, VortexExecutor.class)
        .set(TaskConfiguration.ON_MESSAGE, VortexExecutor.DriverMessageHandler.class)
        .set(TaskConfiguration.ON_CLOSE, VortexExecutor.TaskCloseHandler.class)
        .build();
  }

  private Configuration getNameResolverServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServer.getPort()))
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
  }
}
