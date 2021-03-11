package org.apache.nemo.runtime.executor;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.master.*;
import org.apache.nemo.runtime.master.resource.ContainerManager;
import org.apache.nemo.runtime.master.resource.ResourceSpecification;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.powermock.reflect.Whitebox;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.mock;

public class MasterBuilder {
  private static final Tang TANG = Tang.Factory.getTang();

  public final Map<Triple<String, String, String>, Integer> pipeIndexMap;
  // public final Map<String, String> taskScheduledMap;
  public final PipeIndexMaster pipeIndexMaster;
  // public final TaskScheduledMapWorker taskScheduledMapWorker;
  public final NameServer nameServer;
  public final MessageEnvironment messageEnvironment;
  public final List<String> executorIds = new LinkedList<>();
  public final Map<String, ResourceSpecification> pendingContextIdToResourceSpec;
  public final Map<String, CountDownLatch> requestLatchByResourceSpecId;

  public final RuntimeMaster runtimeMaster;
  public final ContainerManager containerManager;
  public final PhysicalPlanGenerator planGenerator;
  public final TaskScheduledMapMaster taskScheduledMapMaster;

  public MasterBuilder() throws InjectionException {
    this.nameServer = PipeManagerTestHelper.createNameServer();
    final Injector injector = TANG.newInjector(
      PipeManagerTestHelper.createPipeManagerMasterConf(nameServer));

    final ClientRPC clientRPC = mock(ClientRPC.class);
    final EvaluatorRequestor evaluatorRequestor = mock(EvaluatorRequestor.class);
    final JVMProcessFactory jvmProcessFactory = mock(JVMProcessFactory.class);

    injector.bindVolatileInstance(ClientRPC.class, clientRPC);
    injector.bindVolatileInstance(EvaluatorRequestor.class, evaluatorRequestor);
    injector.bindVolatileInstance(JVMProcessFactory.class, jvmProcessFactory);

    this.runtimeMaster = injector.getInstance(RuntimeMaster.class);
    this.pipeIndexMaster = injector.getInstance(PipeIndexMaster.class);
    this.containerManager = injector.getInstance(ContainerManager.class);
    this.planGenerator = injector.getInstance(PhysicalPlanGenerator.class);
    this.taskScheduledMapMaster = injector.getInstance(TaskScheduledMapMaster.class);

    this.pipeIndexMap = Whitebox.getInternalState(pipeIndexMaster, "pipeKeyIndexMap");
    this.pendingContextIdToResourceSpec = Whitebox.getInternalState(containerManager, "pendingContextIdToResourceSpec");
    this.requestLatchByResourceSpecId = Whitebox.getInternalState(containerManager, "requestLatchByResourceSpecId");
    this.messageEnvironment = injector.getInstance(MessageEnvironment.class);
  }

  public void close() throws Exception {
    messageEnvironment.close();
    nameServer.close();
    runtimeMaster.terminate();
  }
}
