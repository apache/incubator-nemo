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
package edu.snu.nemo.client;

import edu.snu.nemo.runtime.common.state.JobState;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A request endpoint in client side of a job.
 */
public abstract class ClientEndpoint {

  /**
   * The request endpoint in driver side of the job.
   */
  private final AtomicReference<DriverEndpoint> driverEndpoint;

  /**
   * A lock and condition to check whether the driver endpoint is connected or not.
   */
  private final Lock connectionLock;
  private final Condition driverConnected;
  private static final long DEFAULT_DRIVER_WAIT_IN_MILLIS = 100;

  /**
   * A {@link StateTranslator} for this job.
   */
  private final StateTranslator stateTranslator;

  /**
   * Constructor.
   * @param stateTranslator translator to translate between the state of job and corresponding.
   */
  public ClientEndpoint(final StateTranslator stateTranslator) {
    this.driverEndpoint = new AtomicReference<>();
    this.connectionLock = new ReentrantLock();
    this.driverConnected = connectionLock.newCondition();
    this.stateTranslator = stateTranslator;
  }

  /**
   * Connect the driver endpoint of this job.
   * This method will be called by {@link DriverEndpoint}.
   *
   * @param dep connected with this client.
   */
  final void connectDriver(final DriverEndpoint dep) {
    connectionLock.lock();
    try {
      this.driverEndpoint.set(dep);
      driverConnected.signalAll();
    } finally {
      connectionLock.unlock();
    }
  }

  /**
   * Wait until the {@link DriverEndpoint} is connected.
   * It wait for at most the given time.
   *
   * @param timeout of waiting.
   * @param unit    of the timeout.
   * @return {@code true} if the manager set.
   */
  private boolean waitUntilConnected(final long timeout,
                                     final TimeUnit unit) {
    connectionLock.lock();
    try {
      if (driverEndpoint.get() == null) {
        // If the driver endpoint is not connected, wait.
        return driverConnected.await(timeout, unit);
      } else {
        return true;
      }
    } catch (final InterruptedException e) {
      e.printStackTrace(System.err);
      Thread.currentThread().interrupt();
      return false;
    } finally {
      connectionLock.unlock();
    }
  }

  /**
   * Wait until the {@link DriverEndpoint} is connected.
   *
   * @return {@code true} if the manager set.
   */
  private boolean waitUntilConnected() {
    connectionLock.lock();
    try {
      if (driverEndpoint.get() == null) {
        // If the driver endpoint is not connected, wait.
        driverConnected.await();
      }
      return true;
    } catch (final InterruptedException e) {
      e.printStackTrace(System.err);
      Thread.currentThread().interrupt();
      return false;
    } finally {
      connectionLock.unlock();
    }
  }

  /**
   * Get the current state of the running job.
   *
   * @return the current state of the running job.
   */
  public final synchronized Enum getJobState() {
    if (driverEndpoint.get() != null) {
      return stateTranslator.translateState(driverEndpoint.get().getState());
    } else {
      return stateTranslator.translateState(JobState.State.READY);
    }
  }

  /**
   * Wait for this job to be finished (complete or failed) and return the final state.
   * It wait for at most the given time.
   *
   * @param timeout of waiting.
   * @param unit    of the timeout.
   * @return the final state of this job.
   */
  public final Enum waitUntilJobFinish(final long timeout,
                                          final TimeUnit unit) {
    if (driverEndpoint.get() != null) {
      return stateTranslator.translateState(driverEndpoint.get().waitUntilFinish(timeout, unit));
    } else {
      // The driver endpoint is not connected yet.
      final long currentNano = System.nanoTime();
      final boolean driverIsConnected;
      if (DEFAULT_DRIVER_WAIT_IN_MILLIS < unit.toMillis(timeout)) {
        driverIsConnected = waitUntilConnected(DEFAULT_DRIVER_WAIT_IN_MILLIS, TimeUnit.MILLISECONDS);
      } else {
        driverIsConnected = waitUntilConnected(timeout, unit);
      }

      if (driverIsConnected) {
        final long consumedTime = System.nanoTime() - currentNano;
        return stateTranslator.translateState(driverEndpoint.get().
            waitUntilFinish(timeout - unit.convert(consumedTime, TimeUnit.NANOSECONDS), unit));
      } else {
        return JobState.State.READY;
      }
    }
  }

  /**
   * Wait for this job to be finished and return the final state.
   *
   * @return the final state of this job.
   */
  public final Enum waitUntilJobFinish() {
    if (driverEndpoint.get() != null) {
      return stateTranslator.translateState(driverEndpoint.get().waitUntilFinish());
    } else {
      // The driver endpoint is not connected yet.
      final boolean driverIsConnected = waitUntilConnected();

      if (driverIsConnected) {
        return stateTranslator.translateState(driverEndpoint.get().waitUntilFinish());
      } else {
        return JobState.State.READY;
      }
    }
  }
}
