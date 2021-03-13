package org.apache.nemo.runtime.executor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.IntDecoderFactory;
import org.apache.nemo.common.coder.IntEncoderFactory;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.datatransfer.PipeManagerWorker;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.common.CyclicDependencyHandler;
import org.apache.nemo.runtime.executor.common.PipeManagerWorkerImpl;
import org.apache.nemo.runtime.executor.common.DefaltIntermediateDataIOFactoryImpl;
import org.apache.nemo.runtime.executor.common.datatransfer.DefaultOutputCollectorGeneratorImpl;
import org.apache.nemo.runtime.executor.offloading.DefaultOffloadingWorkerFactory;
import org.apache.nemo.runtime.executor.offloading.OffloadingWorkerFactory;
import org.apache.nemo.runtime.master.scheduler.Scheduler;
import org.apache.nemo.runtime.master.scheduler.StreamingScheduler;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageParameters;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import java.util.*;

import static org.mockito.Mockito.mock;

public class PipeManagerTestHelper {
  private static final Tang TANG = Tang.Factory.getTang();

  public static NameServer createNameServer() throws InjectionException {
    final Configuration configuration = TANG.newConfigurationBuilder()
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();
    final Injector injector = TANG.newInjector(configuration);
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    final NameServer nameServer = injector.getInstance(NameServer.class);
    return nameServer;
  }

  public static Configuration createGrpcMessageEnvironmentConf(
    final String senderId) {
    return TANG.newConfigurationBuilder()
      .bindNamedParameter(MessageParameters.SenderId.class, senderId)
      .build();
  }

  public static Configuration createNameResolverConf(final NameServer ns) throws InjectionException {
    final Configuration configuration = TANG.newConfigurationBuilder()
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();
    final Injector injector = TANG.newInjector(configuration);
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);

    final Configuration nameClientConfiguration = NameResolverConfiguration.CONF
      .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddressProvider.getLocalAddress())
      .set(NameResolverConfiguration.NAME_SERVICE_PORT, ns.getPort())
      .set(NameResolverConfiguration.IDENTIFIER_FACTORY, StringIdentifierFactory.class)
      .build();
    return nameClientConfiguration;
  }

  public static Configuration createPipeManagerMasterConf(final NameServer ns) throws InjectionException {

    final Configuration conf = Configurations.merge(
      TANG.newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .bindImplementation(Scheduler.class, StreamingScheduler.class)
        .bindNamedParameter(JobConf.JobId.class, "test-job")
        .build(),
      createNameResolverConf(ns),
      createGrpcMessageEnvironmentConf(MessageEnvironment.MASTER_ID));

    return conf;
  }

  public static final Serializer INT_SERIALIZER = new Serializer(IntEncoderFactory.of(),
    IntDecoderFactory.of(), Collections.emptyList(), Collections.emptyList());

  public static Pair<PipeManagerWorker, Injector>
  createPipeManagerWorker(final String executorId,
                          final NameServer nameServer) throws InjectionException {

    final Configuration conf = TANG.newConfigurationBuilder()
      .bindNamedParameter(JobConf.ExecutorId.class, executorId)
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .bindImplementation(PipeManagerWorker.class, PipeManagerWorkerImpl.class)
      .build();

    final Configuration nameResolverConf = PipeManagerTestHelper.createNameResolverConf(nameServer);
    final Configuration grpcConf = PipeManagerTestHelper.createGrpcMessageEnvironmentConf(executorId);

    final Injector injector = TANG.newInjector(Configurations.merge(conf, nameResolverConf, grpcConf));

    final PipeManagerWorker pipeManagerWorker = injector.getInstance(PipeManagerWorker.class);

    injector.bindVolatileInstance(BlockManagerWorker.class, mock(BlockManagerWorker.class));
    final CyclicDependencyHandler dependencyHandler = injector.getInstance(CyclicDependencyHandler.class);

    return Pair.of(pipeManagerWorker, injector);
  }

  public static Pair<Executor, Injector>
  createExecutor(final String executorId,
                 final NameServer nameServer,
                 final StateStore stateStore,
                 final long offloadingThrottleRate,
                 final Map<String, Double> samplingMap,
                 final Class<? extends OffloadingManager> offloadingManager) throws InjectionException, JsonProcessingException {
    final ObjectMapper objectMapper = new ObjectMapper();

    final Configuration conf = TANG.newConfigurationBuilder()
      .bindNamedParameter(JobConf.ExecutorId.class, executorId)
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .bindImplementation(PipeManagerWorker.class, PipeManagerWorkerImpl.class)
      .bindImplementation(ControlEventHandler.class, DefaultControlEventHandlerImpl.class)
      .bindImplementation(SerializerManager.class, DefaultSerializerManagerImpl.class)
      .bindImplementation(IntermediateDataIOFactory.class, DefaltIntermediateDataIOFactoryImpl.class)
      .bindImplementation(OffloadingWorkerFactory.class, DefaultOffloadingWorkerFactory.class)
      .bindImplementation(OffloadingRequesterFactory.class, LocalExecutorOffloadingRequesterFactory.class) // todo: fix
      .bindImplementation(OutputCollectorGenerator.class, DefaultOutputCollectorGeneratorImpl.class)
      .bindImplementation(OffloadingManager.class, offloadingManager)
      .bindNamedParameter(EvalConf.ThrottleRate.class, Long.toString(offloadingThrottleRate))
      .bindNamedParameter(EvalConf.SamplingJsonString.class, objectMapper.writeValueAsString(samplingMap))
      .build();

    final Configuration nameResolverConf = PipeManagerTestHelper.createNameResolverConf(nameServer);
    final Configuration grpcConf = PipeManagerTestHelper.createGrpcMessageEnvironmentConf(executorId);

    final Injector injector = TANG.newInjector(Configurations.merge(conf, nameResolverConf, grpcConf));
    injector.bindVolatileInstance(StateStore.class, stateStore);
    injector.bindVolatileInstance(BlockManagerWorker.class, mock(BlockManagerWorker.class));

    return Pair.of(injector.getInstance(Executor.class), injector);
  }
}
