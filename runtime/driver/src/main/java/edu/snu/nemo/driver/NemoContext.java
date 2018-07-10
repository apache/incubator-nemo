/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.driver;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.executor.Executor;
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
public final class NemoContext {
  private static final Logger LOG = LoggerFactory.getLogger(NemoContext.class.getName());
  private final Executor executor;

  private final Clock clock;
  private final int crashTimeSec;

  @Inject
  private NemoContext(final Executor executor,
                      @Parameter(JobConf.ExecutorPosionSec.class) final int crashTimeSec,
                      final Clock clock) {
    this.executor = executor; // To make Tang instantiate Executor

    // For poison handling
    this.clock = clock;
    this.crashTimeSec = crashTimeSec;
  }

  /**
   * Called when the context starts.
   */
  public final class ContextStartHandler implements EventHandler<ContextStart> {
    @Override
    public void onNext(final ContextStart contextStart) {
      LOG.info("Context Started: Executor is now ready and listening for messages");

      // For poison handling
      if (crashTimeSec >= 0) {
        final int crashTimeMs = addNoise(crashTimeSec * 1000);
        LOG.info("Configured {} sec crash time, and actually crashing in {} ms (noise)", crashTimeSec, crashTimeMs);
        clock.scheduleAlarm(crashTimeMs, (alarm) -> {
          LOG.info("Poison: crashing immediately");
          Runtime.getRuntime().halt(1); // Forces this JVM to shut down immediately.
        });
      }
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

  private int addNoise(final int number) {
    final Random random = new Random();
    final int fiftyPercent = random.nextInt((int) (number * (50.0 / 100.0)));
    return random.nextBoolean() ? number + fiftyPercent : number - fiftyPercent; // -50% ~ +50%
  }
}
