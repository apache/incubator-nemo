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

import org.apache.nemo.common.punctuation.Watermark;
import java.util.Map;

/**
 * State during {@link GBKStreamingTransform}.
 * @param <K> key type
 */
public final class GBKState<K> {

  private final Watermark prevOutputWatermark;
  private final Map<K, Watermark> keyAndWatermarkHoldMap;
  private final Watermark inputWatermark;
  private final InMemoryTimerInternalsFactory<K> timerInternalsFactory;
  private final InMemoryStateInternalsFactory<K> stateInternalsFactory;

  public Watermark getPrevOutputWatermark() {
    return prevOutputWatermark;
  }

  public Map<K, Watermark> getKeyAndWatermarkHoldMap() {
    return keyAndWatermarkHoldMap;
  }

  public Watermark getInputWatermark() {
    return inputWatermark;
  }

  public InMemoryTimerInternalsFactory<K> getTimerInternalsFactory() {
    return timerInternalsFactory;
  }

  public InMemoryStateInternalsFactory<K> getStateInternalsFactory() {
    return stateInternalsFactory;
  }






  public GBKState(final InMemoryTimerInternalsFactory<K> timerInternalsFactory,
                       final InMemoryStateInternalsFactory<K> stateInternalsFactory,
                       final Watermark prevOutputWatermark,
                       final Map<K, Watermark> keyAndWatermarkHoldMap,
                       final Watermark inputWatermark) {
    this.timerInternalsFactory = timerInternalsFactory;
    this.stateInternalsFactory = stateInternalsFactory;
    this.prevOutputWatermark = prevOutputWatermark;
    this.keyAndWatermarkHoldMap = keyAndWatermarkHoldMap;
    this.inputWatermark = inputWatermark;
  }

  @Override
  public String toString() {
    return "TimerInternalsFactory: " + timerInternalsFactory + "\n"
      + "StateInternalsFactory: " + stateInternalsFactory + "\n"
      + "PrevOutputWatermark: " + prevOutputWatermark + "\n"
      + "KeyAndWatermarkHoldMap: " + keyAndWatermarkHoldMap + "\n"
      + "InputWatermark: " + inputWatermark + "\n";
  }
}
