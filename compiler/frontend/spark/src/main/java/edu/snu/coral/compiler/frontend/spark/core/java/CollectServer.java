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
package edu.snu.coral.compiler.frontend.spark.core.java;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.impl.SyncStage;
import org.apache.reef.wake.remote.RemoteConfiguration;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.netty.NettyMessagingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public final class CollectServer {
  private static final Logger LOG = LoggerFactory.getLogger(CollectServer.class);

  private final Transport transport;
  private final int port;

  @Inject
  private CollectServer(final LocalAddressProvider localAddressProvider) {

    final Injector injector = Tang.Factory.getTang().newInjector();

    injector.bindVolatileParameter(RemoteConfiguration.HostAddress.class, localAddressProvider.getLocalAddress());
    injector.bindVolatileParameter(RemoteConfiguration.Port.class, 0);
    injector.bindVolatileParameter(RemoteConfiguration.RemoteServerStage.class,
        new SyncStage<>(new NamingServerHandler(handler, codec)));

    try {
      this.transport = injector.getInstance(NettyMessagingTransport.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }

    this.port = transport.getListeningPort();
    LOG.info("Collect server starting, listening at port {}", this.port);
  }

  private
}
