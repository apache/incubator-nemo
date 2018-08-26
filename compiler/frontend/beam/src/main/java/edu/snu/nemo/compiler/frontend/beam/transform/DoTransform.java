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
package edu.snu.nemo.compiler.frontend.beam.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.runtime.executor.datatransfer.OutputCollectorImpl;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * DoFn transform implementation.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public final class DoTransform<I, O> implements Transform<I, O> {
  private final DoFn doFn;
  private final ObjectMapper mapper;
  private final String serializedOptions;
  private OutputCollector<O> outputCollector;
  private StartBundleContext startBundleContext;
  private FinishBundleContext finishBundleContext;
  private ProcessContext processContext;
  private DoFnInvoker invoker;

  /**
   * DoTransform Constructor.
   *
   * @param doFn    doFn.
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
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
    this.startBundleContext = new StartBundleContext(doFn, serializedOptions);
    this.finishBundleContext = new FinishBundleContext(doFn, outputCollector, serializedOptions);
    this.processContext = new ProcessContext(doFn, outputCollector,
      context.getBroadcastVariables(), context.getTagToAdditionalChildren(), serializedOptions);
    this.invoker = DoFnInvokers.invokerFor(doFn);
    invoker.invokeSetup();
    invoker.invokeStartBundle(startBundleContext);
  }

  @Override
  public void onData(final I data) {
    processContext.setElement(data);
    invoker.invokeProcessElement(processContext);
  }

  @Override
  public void close() {
    invoker.invokeFinishBundle(finishBundleContext);
    invoker.invokeTeardown();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("DoTransform:" + doFn);
    return sb.toString();
  }

  /**
   * StartBundleContext.
   *
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class StartBundleContext<I, O> extends DoFn<I, O>.StartBundleContext {
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    /**
     * StartBundleContext.
     *
     * @param fn                DoFn.
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
   *
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class FinishBundleContext<I, O> extends DoFn<I, O>.FinishBundleContext {
    private final OutputCollector<O> outputCollector;
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    /**
     * Constructor.
     *
     * @param fn                DoFn.
     * @param outputCollector   outputCollector of the DoTransform.
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
   *
   * @param <I> input type.
   * @param <O> output type.
   */
  private static final class ProcessContext<I, O> extends DoFn<I, O>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<I, O> {
    private I input;
    private final OutputCollector<O> outputCollector;
    private final Map sideInputs;
    private final Map<String, String> additionalOutputs;
    private final ObjectMapper mapper;
    private final PipelineOptions options;

    /**
     * ProcessContext Constructor.
     *
     * @param fn                 Dofn.
     * @param outputCollector    OutputCollector.
     * @param broadcastVariables Map for broadcast variables.
     * @param additionalOutputs  Map for TaggedOutputs.
     * @param serializedOptions  Options, serialized.
     */
    ProcessContext(final DoFn<I, O> fn,
                   final OutputCollector<O> outputCollector,
                   final Map broadcastVariables,
                   final Map<String, String> additionalOutputs,
                   final String serializedOptions) {
      fn.super();
      this.outputCollector = outputCollector;
      this.sideInputs = broadcastVariables;
      this.additionalOutputs = additionalOutputs;
      this.mapper = new ObjectMapper();
      try {
        this.options = mapper.readValue(serializedOptions, PipelineOptions.class);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Setter for input element.
     *
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
    public Row asRow(final String id) {
      throw new UnsupportedOperationException();
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
      return PaneInfo.createPane(true, true, PaneInfo.Timing.UNKNOWN);
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
      outputCollector.emit(output);
    }

    @Override
    public <T> void output(final TupleTag<T> tupleTag, final T t) {
      final Object dstVertexId = additionalOutputs.get(tupleTag.getId());

      if (dstVertexId == null) {
        outputCollector.emit((O) t);
      } else {
        outputCollector.emit(additionalOutputs.get(tupleTag.getId()), t);
      }
    }

    @Override
    public <T> void outputWithTimestamp(final TupleTag<T> tupleTag, final T t, final Instant instant) {
      throw new UnsupportedOperationException("output(TupleTag, T, Instant) in ProcessContext under DoTransform");
    }

    @Override
    public BoundedWindow window() {
      // Unbounded windows are not supported for now.
      return GlobalWindow.INSTANCE;
    }

    @Override
    public PaneInfo paneInfo(final DoFn<I, O> doFn) {
      return PaneInfo.createPane(true, true, PaneInfo.Timing.UNKNOWN);
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return options;
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
    public I element(final DoFn<I, O> doFn) {
      return this.input;
    }

    @Override
    public Instant timestamp(final DoFn<I, O> doFn) {
      return Instant.now();
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("restrictionTracker() in ProcessContext under DoTransform");
    }

    @Override
    public TimeDomain timeDomain(final DoFn<I, O> doFn) {
      throw new UnsupportedOperationException("timeDomain() in ProcessContext under DoTransform");
    }

    @Override
    public DoFn.OutputReceiver<O> outputReceiver(final DoFn<I, O> doFn) {
      return new OutputReceiver<>((OutputCollectorImpl) outputCollector);
    }

    @Override
    public DoFn.OutputReceiver<Row> outputRowReceiver(final DoFn<I, O> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn.MultiOutputReceiver taggedOutputReceiver(final DoFn<I, O> doFn) {
      return new MultiOutputReceiver((OutputCollectorImpl) outputCollector, additionalOutputs);
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

  /**
   * @return {@link DoFn} for this transform.
   */
  public DoFn getDoFn() {
    return doFn;
  }

  /**
   * OutputReceiver class.
   * @param <O> output type
   */
  static final class OutputReceiver<O> implements DoFn.OutputReceiver<O> {
    private final List<O> dataElements;

    OutputReceiver(final OutputCollectorImpl<O> outputCollector) {
      this.dataElements = outputCollector.getMainTagOutputQueue();
    }

    OutputReceiver(final OutputCollectorImpl outputCollector,
                   final TupleTag<O> tupleTag,
                   final Map<String, String> tagToVertex) {
      final Object dstVertexId = tagToVertex.get(tupleTag.getId());
      if (dstVertexId == null) {
        this.dataElements = outputCollector.getMainTagOutputQueue();
      } else {
        this.dataElements = (List<O>) outputCollector.getAdditionalTagOutputQueue((String) dstVertexId);
      }
    }

    @Override
    public void output(final O output) {
      dataElements.add(output);
    }

    @Override
    public void outputWithTimestamp(final O output, final Instant timestamp) {
      dataElements.add(output);
    }
  }

  /**
   * MultiOutputReceiver class.
   */
  static final class MultiOutputReceiver implements DoFn.MultiOutputReceiver {
    private final OutputCollectorImpl outputCollector;
    private final Map<String, String> tagToVertex;

    /**
     * Constructor.
     * @param outputCollector outputCollector
     * @param tagToVertex     tag to vertex map
     */
    MultiOutputReceiver(final OutputCollectorImpl outputCollector,
                               final Map<String, String> tagToVertex) {
      this.outputCollector = outputCollector;
      this.tagToVertex = tagToVertex;
    }

    @Override
    public <T> DoFn.OutputReceiver<T> get(final TupleTag<T> tag) {
      return new OutputReceiver<>(this.outputCollector, tag, tagToVertex);
    }

    @Override
    public <T> OutputReceiver<Row> getRowReceiver(final TupleTag<T> tag) {
      throw new UnsupportedOperationException();
    }
  }
}
