package org.apache.nemo.runtime.executor.data;

import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransportChannelInitializer;
import org.apache.nemo.runtime.executor.common.datatransfer.VMScalingClientTransport;
import org.apache.nemo.runtime.master.PipeManagerMaster;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverCacheTimeout;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class PipeManagerWorkerTest {

  private static final Tang TANG = Tang.Factory.getTang();
  private NameServer nameServer;
  private PipeManagerMaster pipeManagerMaster;

  @Before
  public void setUp() throws InjectionException {

    // Name server
    nameServer = createNameServer();
    pipeManagerMaster = TANG.newInjector(createPipeManagerMasterConf())
      .getInstance(PipeManagerMaster.class);
  }

  @After
  public void tearDown() throws Exception {
    nameServer.close();
  }

  @Test
  public void sendDataTest() throws InjectionException, InterruptedException {
    // worker 1 -> worker 2
    final PipeManagerWorker pipeManagerWorker1 = createPipeManagerWorker("executor1");
    final PipeManagerWorker pipeManagerWorker2 = createPipeManagerWorker("executor1");

    // 1. init input pipe of worker2
    final String runtimeEdgeId = "edge1";
    final long dstTaskIndex = 1;
    final Pair<String, Long> key = Pair.of(runtimeEdgeId, dstTaskIndex);

    pipeManagerWorker2.notifyMaster(runtimeEdgeId, dstTaskIndex);
    Thread.sleep(2000);

    // Check whether the input pipe is initialized and registered in the master
    assertTrue(pipeManagerMaster.getRuntimeEdgeIndexToExecutor().containsKey(key));

    // 2. send data from worker1 to worker2

  }

  private NameServer createNameServer() throws InjectionException {
    final Configuration configuration = TANG.newConfigurationBuilder()
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();
    final Injector injector = TANG.newInjector(configuration);
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);
    final NameServer nameServer = injector.getInstance(NameServer.class);
    return nameServer;
  }

  private Configuration createNameResolverConf() throws InjectionException {
    final Configuration configuration = TANG.newConfigurationBuilder()
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();
    final Injector injector = TANG.newInjector(configuration);
    final LocalAddressProvider localAddressProvider = injector.getInstance(LocalAddressProvider.class);

    final Configuration nameClientConfiguration = NameResolverConfiguration.CONF
      .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddressProvider.getLocalAddress())
      .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
      .set(NameResolverConfiguration.IDENTIFIER_FACTORY, StringIdentifierFactory.class)
      .build();
    return nameClientConfiguration;
  }

  private Configuration createGrpcMessageEnvironmentConf(
    final String senderId) {
    return TANG.newConfigurationBuilder()
      .bindNamedParameter(MessageParameters.SenderId.class, senderId)
      .build();
  }

  private Configuration createPipeManagerMasterConf() throws InjectionException {

    final Configuration conf = Configurations.merge(
      TANG.newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build(),
      createNameResolverConf(),
      createGrpcMessageEnvironmentConf(MessageEnvironment.MASTER_COMMUNICATION_ID));

    return conf;
  }


  private PipeManagerWorker createPipeManagerWorker(final String executorId) throws InjectionException {

    final Configuration conf = TANG.newConfigurationBuilder()
      .bindNamedParameter(JobConf.ExecutorId.class, executorId)
      .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
      .build();

    final Configuration nameResolverConf = createNameResolverConf();
    final Configuration grpcConf = createGrpcMessageEnvironmentConf(executorId);

    final Injector injector = TANG.newInjector(Configurations.merge(conf, nameResolverConf, grpcConf));

    final PipeManagerWorker pipeManagerWorker = injector.getInstance(PipeManagerWorker.class);

    injector.bindVolatileInstance(BlockManagerWorker.class, mock(BlockManagerWorker.class));
    final CyclicDependencyHandler dependencyHandler = injector.getInstance(CyclicDependencyHandler.class);

    return pipeManagerWorker;
  }
}
