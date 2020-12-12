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
package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * InMemoryTimerInternalsFactory.
 * @param <K> key type
 */
public final class InMemoryTimerInternalsFactory<K> implements TimerInternalsFactory<K> {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTimerInternalsFactory.class.getName());
  private Map<K, InMemoryTimerInternals> timerInternalsMap = new HashMap<>();

  @Override
  public String toString() {
    return "TimerInternalsMap: " + timerInternalsMap;
  }

  @Override
  public TimerInternals timerInternalsForKey(final K key) {
    if (timerInternalsMap.get(key) != null) {
      return timerInternalsMap.get(key);
    } else {
      final InMemoryTimerInternals internal = new InMemoryTimerInternals();
      timerInternalsMap.put(key, internal);
      return internal;
    }
  }

  /**
   * Remove the next eligible timer in {@code timeDomain}.
   *
   * @param timerInternal the timer internals.
   * @param timeDomain the time domain.
   * @return the timer data.
   */
  public TimerInternals.TimerData pollTimer(final InMemoryTimerInternals timerInternal, final TimeDomain timeDomain) {
    switch (timeDomain) {
      case EVENT_TIME :
        return timerInternal.removeNextEventTimer();
      case PROCESSING_TIME:
        return timerInternal.removeNextProcessingTimer();
      case SYNCHRONIZED_PROCESSING_TIME:
        return timerInternal.removeNextSynchronizedProcessingTimer();
      default :
        return null;
    }
  }

  /**
   * Accessor for timerInternalsMap.
   *
   * @return the timer internals map.
   */
  public Map<K, InMemoryTimerInternals> getTimerInternalsMap() {
    return timerInternalsMap;
  }

  /**
   * Helper method to check if {@code timerInternals} doesn't have any timers left.
   *
   * @param timerInternal the in-memory timer internals.
   * @return whether it is empty or not.
   */
  public boolean isEmpty(final InMemoryTimerInternals timerInternal) {
    for (final TimeDomain domain : TimeDomain.values()) {
      if (timerInternal.getNextTimer(domain) != null) {
        return false;
      }
    }
    return true;
  }
}
