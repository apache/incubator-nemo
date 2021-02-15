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
package org.apache.nemo.driver;

import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.Executor;
import org.apache.nemo.runtime.executor.VMWorkerExecutor;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.evaluator.context.events.ContextStart;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Random;

/**
 * REEF Context for the Executor.
 */
@EvaluatorSide
@Unit
public final class OffloadingContext {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingContext.class.getName());
  private final VMWorkerExecutor executor;

  @Inject
  private OffloadingContext(final VMWorkerExecutor executor) {
    this.executor = executor; // To make Tang instantiate Executor
  }

  /**
   * Called when the context starts.
   */
  public final class ContextStartHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      LOG.info("Context Started: Executor is now ready and listening for messages");
    }
  }

  /**
   * Called when the context is stopped.
   */
  public final class ContextStopHandler implements EventHandler<ContextStop> {
    @Override
    public void onNext(final ContextStop contextStop) {
      executor.terminate();
    }
  }
}
