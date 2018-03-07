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
package edu.snu.nemo.compiler.frontend.beam.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DoFn transform implementation.
 * @param <I> input type.
 * @param <O> output type.
 */
public final class DoTransform<I, O> implements Transform<I, O> {
  private final DoFn doFn;
  private final ObjectMapper mapper;
  private final String serializedOptions;
  private Map<PCollectionView, Object> sideInputs;
  private OutputCollector<O> outputCollector;
  private StartBundleContext startBundleContext;
  private FinishBundleContext finishBundleContext;
  private ProcessContext processContext;
  private DoFnInvoker invoker;

  /**
   * DoTransform Constructor.
   * @param doFn doFn.
   * @param options Pipeline options.
   */
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
  public void prepare(final Context context, final OutputCollector<O> p) {
    this.outputCollector = p;
    this.sideInputs = new HashMap<>();
    context.getSideInputs().forEach((k, v) -> this.sideInputs.put(((CreateViewTransform) k).getTag(), v));
    this.startBundleContext = new StartBundleContext(doFn, serializedOptions);
    this.finishBundleContext = new FinishBundleContext(doFn, outputCollector, serializedOptions);
    this.processContext = new ProcessContext(doFn, outputCollector, sideInputs, serializedOptions);
    this.invoker = DoFnInvokers.invokerFor(doFn);
  }

  @Override
  public void onData(final Object data) {
    invoker.invokeSetup();
    invoker.invokeStartBundle(startBundleContext);
    if (data instanceof Iterable) {
      ((Iterable) data).forEach(element -> {
        processContext.setElement(element);
        invoker.invokeProcessElement(processContext);
      });
    } else {
      processContext.setElement(data);
      invoker.invokeProcessElement(processContext);
    }
    invoker.invokeFinishBundle(finishBundleContext);
    invoker.invokeTeardown();
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransform:" + doFn);
    return sb.toString();
  }

  /**
   * StartBundleContext.
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class StartBundleContext<I, O> extends DoFn<I, O>.StartBundleContext {
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    /**
     * StartBundleContext.
     * @param fn DoFn.
     * @param serializedOptions serialized options of the DoTransform.
     */
    StartBundleContext(final DoFn<I, O> fn,
                       final String serializedOptions) {
      fn.super();
      this.mapper = new ObjectMapper();
      try {
        this.options = mapper.readValue(serializedOptions, PipelineOptions.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }
  }

  /**
   * FinishBundleContext.
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class FinishBundleContext<I, O> extends DoFn<I, O>.FinishBundleContext {
    private final OutputCollector<O> outputCollector;
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    /**
     * Constructor.
     * @param fn DoFn.
     * @param outputCollector outputCollector of the DoTransform.
     * @param serializedOptions serialized options of the DoTransform.
     */
    FinishBundleContext(final DoFn<I, O> fn,
                        final OutputCollector<O> outputCollector,
                        final String serializedOptions) {
      fn.super();
      this.outputCollector = outputCollector;
      this.mapper = new ObjectMapper();
      try {
        this.options = mapper.readValue(serializedOptions, PipelineOptions.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public void output(final O output, final Instant instant, final BoundedWindow boundedWindow) {
      outputCollector.emit(output);
    }

    @Override
    public <T> void output(final TupleTag<T> tupleTag,
                           final T t,
                           final Instant instant,
                           final BoundedWindow boundedWindow) {
      throw new UnsupportedOperationException("output(TupleTag, T, Instant, BoundedWindow)"
          + "in FinishBundleContext under DoTransform");
    }
  }

  /**
   * ProcessContext class. Reference: SimpleDoFnRunner.DoFnProcessContext in BEAM.
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class ProcessContext<I, O> extends DoFn<I, O>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<I, O> {
    private I input;
    private final OutputCollector<O> outputCollector;
    private final Map<PCollectionView, Object> sideInputs;
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    /**
     * ProcessContext Constructor.
     * @param fn Dofn.
     * @param outputCollector OutputCollector.
     * @param sideInputs Map for SideInputs.
     * @param serializedOptions Options, serialized.
     */
    ProcessContext(final DoFn<I, O> fn,
                   final OutputCollector<O> outputCollector,
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

    /**
     * Setter for input element.
     * @param in input element.
     */
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
    public void updateWatermark(final Instant instant) {
      throw new UnsupportedOperationException("updateWatermark() in ProcessContext under DoTransform");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return this.options;
    }

    @Override
    public void output(final O output) {
      outputCollector.emit(output);
    }

    @Override
    public void outputWithTimestamp(final O output, final Instant timestamp) {
      throw new UnsupportedOperationException("outputWithTimestamp() in ProcessContext under DoTransform");
    }

    @Override
    public <T> void output(final TupleTag<T> tupleTag, final T t) {
      throw new UnsupportedOperationException("output(TupleTag, T) in ProcessContext under DoTransform");
    }

    @Override
    public <T> void outputWithTimestamp(final TupleTag<T> tupleTag, final T t, final Instant instant) {
      throw new UnsupportedOperationException("output(TupleTag, T, Instant) in ProcessContext under DoTransform");
    }

    @Override
    public BoundedWindow window() {
      return new BoundedWindow() {
        @Override
        public Instant maxTimestamp() {
          return GlobalWindow.INSTANCE.maxTimestamp();
        }
      };
    }

    @Override
    public DoFn<I, O>.StartBundleContext startBundleContext(final DoFn<I, O> doFn) {
      throw new UnsupportedOperationException("StartBundleContext parameters are not supported.");
    }

    @Override
    public DoFn<I, O>.FinishBundleContext finishBundleContext(final DoFn<I, O> doFn) {
      throw new UnsupportedOperationException("FinishBundleContext parameters are not supported.");
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
    public RestrictionTracker<?> restrictionTracker() {
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
