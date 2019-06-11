package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.nemo.common.punctuation.Watermark;

import java.util.Map;

public final class GBKFinalState<K> {

  public final Watermark prevOutputWatermark;
  public final Map<K, Watermark> keyAndWatermarkHoldMap;
  public final Watermark inputWatermark;
  public final InMemoryTimerInternalsFactory<K> timerInternalsFactory;
  public final InMemoryStateInternalsFactory<K> stateInternalsFactory;
  public final Watermark prevKeyAndWatermarkHold;

  public GBKFinalState(final InMemoryTimerInternalsFactory<K> timerInternalsFactory,
                       final InMemoryStateInternalsFactory<K> stateInternalsFactory,
                       final Watermark prevOutputWatermark,
                       final Map<K, Watermark> keyAndWatermarkHoldMap,
                       final Watermark inputWatermark,
                       final Watermark prevKeyAndWatermarkHold) {
    this.timerInternalsFactory = timerInternalsFactory;
    this.stateInternalsFactory = stateInternalsFactory;
    this.prevOutputWatermark = prevOutputWatermark;
    this.keyAndWatermarkHoldMap = keyAndWatermarkHoldMap;
    this.inputWatermark = inputWatermark;
    this.prevKeyAndWatermarkHold = prevKeyAndWatermarkHold;
  }

  @Override
  public String toString() {
    return "TimerInternalsFactory: " + timerInternalsFactory + "\n"
      + "StateInternalsFactory: " + stateInternalsFactory + "\n"
      + "PrevOutputWatermark: " + prevOutputWatermark + "\n"
      + "KeyAndWatermarkHoldMap: " + keyAndWatermarkHoldMap + "\n"
      + "InputWatermark: " + inputWatermark + "\n"
      + "PrevKeyAndWatermark: " + prevKeyAndWatermarkHold + "\n";
  }
}
