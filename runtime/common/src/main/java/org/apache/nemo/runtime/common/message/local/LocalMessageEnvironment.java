/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.message.local;

import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * A simple {@link MessageEnvironment} implementation that works on a single node.
 * Used for unit tests.
 */
public final class LocalMessageEnvironment implements MessageEnvironment {
  private static final Tang TANG = Tang.Factory.getTang();
  public static final Configuration LOCAL_MESSAGE_ENVIRONMENT_CONFIGURATION = TANG.newConfigurationBuilder()
      .bindImplementation(MessageEnvironment.class, LocalMessageEnvironment.class).build();

  private final String currentNodeId;
  private final LocalMessageDispatcher dispatcher;

  @Inject
  private LocalMessageEnvironment(@Parameter(MessageParameters.SenderId.class) final String currentNodeId,
                                  final LocalMessageDispatcher dispatcher) {
    this.currentNodeId = currentNodeId;
    this.dispatcher = dispatcher;
  }

  @Override
  public <T> void setupListener(
      final String listenerId, final MessageListener<T> listener) {
    dispatcher.setupListener(currentNodeId, listenerId, listener);
  }

  @Override
  public void removeListener(final String listenerId) {
    dispatcher.removeListener(currentNodeId, listenerId);
  }

  @Override
  public <T> Future<MessageSender<T>> asyncConnect(
      final String targetId, final String messageTypeId) {
    return CompletableFuture.completedFuture(new LocalMessageSender<T>(
        currentNodeId, targetId, messageTypeId, dispatcher));
  }

  @Override
  public String getId() {
    return currentNodeId;
  }

  @Override
  public void close() {
    // No-ops.
  }

  /**
   * Extends {@code baseInjector} to have {@link LocalMessageEnvironment} instance for the given {@code senderId}.
   *
   * @param baseInjector provided by {@link LocalMessageDispatcher#getInjector()}
   *                     or {@link LocalMessageDispatcher#forkInjector(Injector)}
   * @param senderId  the identifier for the sender
   * @return an {@link Injector} which has {@link LocalMessageDispatcher} instance for {@link MessageEnvironment}
   * @throws InjectionException when fails to inject {@link MessageEnvironment}
   */
  public static Injector forkInjector(final Injector baseInjector, final String senderId) throws InjectionException {
    final Injector injector = baseInjector.forkInjector(TANG.newConfigurationBuilder()
        .bindNamedParameter(MessageParameters.SenderId.class, senderId).build());
    injector.getInstance(MessageEnvironment.class);
    return injector;
  }
}
