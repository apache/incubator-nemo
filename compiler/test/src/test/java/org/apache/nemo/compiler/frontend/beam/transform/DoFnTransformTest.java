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
package org.apache.nemo.compiler.frontend.beam.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.reef.io.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class DoFnTransformTest {

  // views and windows for testing side inputs
  private PCollectionView<Iterable<String>> view1;
  private PCollectionView<Iterable<String>> view2;

  private final static Coder NULL_INPUT_CODER = null;
  private final static Map<TupleTag<?>, Coder<?>> NULL_OUTPUT_CODERS = null;

  @Before
  public void setUp() {
    Pipeline.create().apply(Create.of("1"));
    view1 = Pipeline.create().apply(Create.of("1")).apply(View.asIterable());
    view2 = Pipeline.create().apply(Create.of("2")).apply(View.asIterable());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSingleOutput() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");

    final DoFnTransform<String, String> doFnTransform =
      new DoFnTransform<>(
        new IdentityDoFn<>(),
        NULL_INPUT_CODER,
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));

    final Transform.Context context = mock(Transform.Context.class);
    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.valueInGlobalWindow("Hello"));

    assertEquals(((TestOutputCollector<String>) oc).outputs.get(0), WindowedValue.valueInGlobalWindow("Hello"));

    doFnTransform.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultiOutputOutput() {

    TupleTag<String> mainOutput = new TupleTag<>("main-output");
    TupleTag<String> additionalOutput1 = new TupleTag<>("output-1");
    TupleTag<String> additionalOutput2 = new TupleTag<>("output-2");

    ImmutableList<TupleTag<?>> tags = ImmutableList.of(additionalOutput1, additionalOutput2);

    ImmutableMap<String, String> tagsMap =
      ImmutableMap.<String, String>builder()
        .put(additionalOutput1.getId(), additionalOutput1.getId())
        .put(additionalOutput2.getId(), additionalOutput2.getId())
        .build();

    final DoFnTransform<String, String> doFnTransform =
      new DoFnTransform<>(
        new MultiOutputDoFn(additionalOutput1, additionalOutput2),
        NULL_INPUT_CODER,
        NULL_OUTPUT_CODERS,
        mainOutput,
        tags,
        WindowingStrategy.globalDefault(),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));

    // mock context
    final Transform.Context context = mock(Transform.Context.class);
    when(context.getTagToAdditionalChildren()).thenReturn(tagsMap);

    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.valueInGlobalWindow("one"));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow("two"));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow("hello"));

    // main output
    assertEquals(WindowedValue.valueInGlobalWindow("got: hello"),
      ((TestOutputCollector<String>) oc).outputs.get(0));

    // additional output 1
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput1.getId(), WindowedValue.valueInGlobalWindow("extra: one"))
    ));
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput1.getId(), WindowedValue.valueInGlobalWindow("got: hello"))
    ));

    // additional output 2
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput2.getId(), WindowedValue.valueInGlobalWindow("extra: two"))
    ));
    assertTrue(((TestOutputCollector<String>) oc).getTaggedOutputs().contains(
      new Tuple<>(additionalOutput2.getId(), WindowedValue.valueInGlobalWindow("got: hello"))
    ));

    doFnTransform.close();
  }

  // TODO #216: implement side input and windowing
  @Test
  public void testSideInputs() {
    // mock context
    final Transform.Context context = mock(Transform.Context.class);
    when(context.getBroadcastVariable(view1)).thenReturn(
      WindowedValue.valueInGlobalWindow(ImmutableList.of("1")));
    when(context.getBroadcastVariable(view2)).thenReturn(
      WindowedValue.valueInGlobalWindow(ImmutableList.of("2")));

    TupleTag<Tuple<String, Iterable<String>>> outputTag = new TupleTag<>("main-output");

    WindowedValue<String> first = WindowedValue.valueInGlobalWindow("first");
    WindowedValue<String> second = WindowedValue.valueInGlobalWindow("second");

    final Map<String, PCollectionView<Iterable<String>>> eventAndViewMap =
      ImmutableMap.of(first.getValue(), view1, second.getValue(), view2);

    final DoFnTransform<String, Tuple<String, Iterable<String>>> doFnTransform =
      new DoFnTransform<>(
        new SimpleSideInputDoFn<>(eventAndViewMap),
        NULL_INPUT_CODER,
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        ImmutableList.of(view1, view2), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class));

    final OutputCollector<WindowedValue<Tuple<String, Iterable<String>>>> oc = new TestOutputCollector<>();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(first);
    doFnTransform.onData(second);

    assertEquals(WindowedValue.valueInGlobalWindow(new Tuple<>("first", ImmutableList.of("1"))),
      ((TestOutputCollector<Tuple<String,Iterable<String>>>) oc).getOutput().get(0));

    assertEquals(WindowedValue.valueInGlobalWindow(new Tuple<>("second", ImmutableList.of("2"))),
      ((TestOutputCollector<Tuple<String,Iterable<String>>>) oc).getOutput().get(1));

    doFnTransform.close();
  }

  private static final class TestOutputCollector<T> implements OutputCollector<WindowedValue<T>> {
    private final List<WindowedValue<T>> outputs;
    private final List<Tuple<String, WindowedValue<T>>> taggedOutputs;

    TestOutputCollector() {
      this.outputs = new LinkedList<>();
      this.taggedOutputs = new LinkedList<>();
    }

    @Override
    public void emit(WindowedValue<T> output) {
      outputs.add(output);
    }

    @Override
    public <O> void emit(String dstVertexId, O output) {
      final WindowedValue<T> val = (WindowedValue<T>) output;
      final Tuple<String, WindowedValue<T>> tuple = new Tuple<>(dstVertexId, val);
      taggedOutputs.add(tuple);
    }

    public List<WindowedValue<T>> getOutput() {
      return outputs;
    }

    public List<Tuple<String, WindowedValue<T>>> getTaggedOutputs() {
      return taggedOutputs;
    }
  }

  /**
   * Identitiy do fn.
   * @param <T> type
   */
  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      c.output(c.element());
    }
  }

  /**
   * Side input do fn.
   * @param <T> type
   */
  private static class SimpleSideInputDoFn<T, V> extends DoFn<T, Tuple<T, V>> {
    private final Map<T, PCollectionView<V>> idAndViewMap;

    public SimpleSideInputDoFn(final Map<T, PCollectionView<V>> idAndViewMap) {
      this.idAndViewMap = idAndViewMap;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final PCollectionView<V> view = idAndViewMap.get(c.element());
      final V sideInput = c.sideInput(view);
      c.output(new Tuple<>(c.element(), sideInput));
    }
  }


  /**
   * Multi output do fn.
   */
  private static class MultiOutputDoFn extends DoFn<String, String> {
    private TupleTag<String> additionalOutput1;
    private TupleTag<String> additionalOutput2;

    public MultiOutputDoFn(TupleTag<String> additionalOutput1, TupleTag<String> additionalOutput2) {
      this.additionalOutput1 = additionalOutput1;
      this.additionalOutput2 = additionalOutput2;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if ("one".equals(c.element())) {
        c.output(additionalOutput1, "extra: one");
      } else if ("two".equals(c.element())) {
        c.output(additionalOutput2, "extra: two");
      } else {
        c.output("got: " + c.element());
        c.output(additionalOutput1, "got: " + c.element());
        c.output(additionalOutput2, "got: " + c.element());
      }
    }
  }
}
