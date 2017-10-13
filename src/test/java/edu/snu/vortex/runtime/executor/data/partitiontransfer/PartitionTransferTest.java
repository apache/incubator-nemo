/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.data.partitiontransfer;

import edu.snu.vortex.client.JobConf;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifier;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.junit.Test;

public final class PartitionTransferTest {
  private static final Tang TANG = Tang.Factory.getTang();
  @Test
  public void test() throws InjectionException {
    final Configuration driverConf = TANG.newConfigurationBuilder()
        .build();
    final Injector driverInjector = TANG.newInjector(driverConf);
    final NameServer nameServer = driverInjector.getInstance(NameServer.class);
    final LocalAddressProvider localAddressProvider = driverInjector.getInstance(LocalAddressProvider.class);

    final PartitionTransfer transfer = createTransfer("a",
        localAddressProvider.getLocalAddress(), nameServer.getPort());
  }

  private static PartitionTransfer createTransfer(
      final String executorId,
      final String nameServerHost,
      final int nameServerPort) throws InjectionException {
    final Configuration executorConf = TANG.newConfigurationBuilder()
        .bindNamedParameter(JobConf.ExecutorId.class, executorId)
        .bindNamedParameter(NameResolverNameServerAddr.class, nameServerHost)
        .bindNamedParameter(NameResolverNameServerPort.class, String.valueOf(nameServerPort))
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
    final Injector executorInjector = TANG.newInjector(executorConf);

    final PartitionTransfer partitionTransfer = executorInjector.getInstance(PartitionTransfer.class);
    return partitionTransfer;
  }
}
