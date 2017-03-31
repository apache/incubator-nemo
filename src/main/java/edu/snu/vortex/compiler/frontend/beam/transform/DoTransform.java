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
package edu.snu.vortex.compiler.frontend.beam.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.vortex.compiler.frontend.beam.BeamElement;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.OutputCollector;
import edu.snu.vortex.compiler.ir.Transform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DoFn transform implementation.
 */
public final class DoTransform implements Transform {
  private final DoFn doFn;
  private final ObjectMapper mapper;
  private final String serializedOptions;
  private Map<PCollectionView, Object> sideInputs;
  private OutputCollector outputCollector;

  public DoTransform(final DoFn doFn, final PipelineOptions options) {
    this.doFn = doFn;
    this.mapper = new ObjectMapper();
    try {
      this.serializedOptions = mapper.writeValueAsString(options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void prepare(final Context context, final OutputCollector oc) {
    this.outputCollector = oc;
    this.sideInputs = new HashMap<>();
    context.getSideInputs().forEach((k, v) -> this.sideInputs.put(((BroadcastTransform) k).getTag(), v));
  }

  @Override
  public void onData(final Iterable<Element> data, final String srcVertexId) {
    final ProcessContext beamContext = new ProcessContext(doFn, outputCollector, sideInputs, serializedOptions);
    final DoFnInvoker invoker = DoFnInvokers.invokerFor(doFn);
    invoker.invokeSetup();
    invoker.invokeStartBundle(beamContext);
    data.forEach(element -> { // No need to check for input index, since it is always 0 for DoTransform
      beamContext.setElement(element.getData());
      invoker.invokeProcessElement(beamContext);
    });
    invoker.invokeFinishBundle(beamContext);
    invoker.invokeTeardown();
  }

  @Override
  public void close() {
    // do nothing
  }


  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransform:" + doFn);
    return sb.toString();
  }

  /**
   * ProcessContext class. Reference: SimpleDoFnRunner.DoFnProcessContext in BEAM.
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class ProcessContext<I, O> extends DoFn<I, O>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<I, O> {
    private I input;
    private final OutputCollector outputCollector;
    private final Map<PCollectionView, Object> sideInputs;
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    ProcessContext(final DoFn<I, O> fn,
                   final OutputCollector outputCollector,
                   final Map<PCollectionView, Object> sideInputs,
                   final String serializedOptions) {
      fn.super();
      this.outputCollector = outputCollector;
      this.sideInputs = sideInputs;
      this.mapper = new ObjectMapper();
      try {
        this.options = mapper.readValue(serializedOptions, PipelineOptions.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void setElement(final I in) {
      this.input = in;
    }

    @Override
    public I element() {
      return this.input;
    }

    @Override
    public <T> T sideInput(final PCollectionView<T> view) {
      return (T) sideInputs.get(view);
    }

    @Override
    public Instant timestamp() {
      throw new UnsupportedOperationException("timestamp() in ProcessContext under DoTransform");
    }

    @Override
    public PaneInfo pane() {
      throw new UnsupportedOperationException("pane() in ProcessContext under DoTransform");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return this.options;
    }

    @Override
    public void output(final O output) {
      outputCollector.emit(new BeamElement<>(output));
    }

    @Override
    public void outputWithTimestamp(final O output, final Instant timestamp) {
      throw new UnsupportedOperationException("outputWithTimestamp() in ProcessContext under DoTransform");
    }

    @Override
    public <T> void sideOutput(final TupleTag<T> tag, final T output) {
      throw new UnsupportedOperationException("sideOutput() in ProcessContext under DoTransform");
    }

    @Override
    public <T> void sideOutputWithTimestamp(final TupleTag<T> tag, final T output, final Instant timestamp) {
      throw new UnsupportedOperationException("sideOutputWithTimestamp() in ProcessContext under DoTransform");
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        final String name, final Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      throw new UnsupportedOperationException("createAggregator() in ProcessContext under DoTransform");
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException("window() in ProcessContext under DoTransform");
    }

    @Override
    public DoFn.Context context(final DoFn<I, O> doFn) {
      return this;
    }

    @Override
    public DoFn.ProcessContext
        processContext(final DoFn<I, O> doFn) {
      return this;
    }

    @Override
    public DoFn.OnTimerContext
        onTimerContext(final DoFn<I, O> doFn) {
      throw new UnsupportedOperationException("onTimerContext() in ProcessContext under DoTransform");
    }

    @Override
    public DoFn.InputProvider<I> inputProvider() {
      throw new UnsupportedOperationException("inputProvider() in ProcessContext under DoTransform");
    }

    @Override
    public DoFn.OutputReceiver<O> outputReceiver() {
      throw new UnsupportedOperationException("outputReceiver() in ProcessContext under DoTransform");
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
      throw new UnsupportedOperationException("windowingInternals() in ProcessContext under DoTransform");
    }

    @Override
    public <RestrictionT> RestrictionTracker<RestrictionT> restrictionTracker() {
      throw new UnsupportedOperationException("restrictionTracker() in ProcessContext under DoTransform");
    }

    @Override
    public State state(final String stateId) {
      throw new UnsupportedOperationException("state() in ProcessContext under DoTransform");
    }

    @Override
    public Timer timer(final String timerId) {
      throw new UnsupportedOperationException("timer() in ProcessContext under DoTransform");
    }
  }
}

