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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;
import org.apache.reef.io.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

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
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        DisplayData.none());

    final Transform.Context context = mock(Transform.Context.class);
    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.valueInGlobalWindow("Hello"));

    assertEquals(((TestOutputCollector<String>) oc).outputs.get(0), WindowedValue.valueInGlobalWindow("Hello"));

    doFnTransform.close();
  }


  @Test
  @SuppressWarnings("unchecked")
  public void testCountBundle() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final NemoPipelineOptions pipelineOptions = PipelineOptionsFactory.as(NemoPipelineOptions.class);
    pipelineOptions.setMaxBundleSize(3L);
    pipelineOptions.setMaxBundleTimeMills(10000000L);

    final List<Integer> bundleOutput = new ArrayList<>();

    final DoFnTransform<String, String> doFnTransform =
      new DoFnTransform<>(
        new BundleTestDoFn(bundleOutput),
        NULL_INPUT_CODER,
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        pipelineOptions,
        DisplayData.none());

    final Transform.Context context = mock(Transform.Context.class);
    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));

    assertEquals(3, (int) bundleOutput.get(0));

    bundleOutput.clear();

    doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));

    assertEquals(3, (int) bundleOutput.get(0));

    doFnTransform.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTimeBundle() {

    final long maxBundleTimeMills = 1000L;
    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final NemoPipelineOptions pipelineOptions = PipelineOptionsFactory.as(NemoPipelineOptions.class);
    pipelineOptions.setMaxBundleSize(10000000L);
    pipelineOptions.setMaxBundleTimeMills(maxBundleTimeMills);

    final List<Integer> bundleOutput = new ArrayList<>();

    final DoFnTransform<String, String> doFnTransform =
      new DoFnTransform<>(
        new BundleTestDoFn(bundleOutput),
        NULL_INPUT_CODER,
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        pipelineOptions,
        DisplayData.none());

    final Transform.Context context = mock(Transform.Context.class);
    final OutputCollector<WindowedValue<String>> oc = new TestOutputCollector<>();

    long startTime = System.currentTimeMillis();
    doFnTransform.prepare(context, oc);

    int count = 0;
    while (bundleOutput.isEmpty()) {
      doFnTransform.onData(WindowedValue.valueInGlobalWindow("a"));
      count += 1;
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    long endTime = System.currentTimeMillis();
    assertEquals(count, (int) bundleOutput.get(0));
    assertTrue(endTime - startTime >= maxBundleTimeMills);

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
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        DisplayData.none());

    // mock context
    final Transform.Context context = mock(Transform.Context.class);

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

  @Test
  public void testSideInputs() {
    // mock context
    final Transform.Context context = mock(Transform.Context.class);
    TupleTag<Tuple<String, Iterable<String>>> outputTag = new TupleTag<>("main-output");

    WindowedValue<String> firstElement = WindowedValue.valueInGlobalWindow("first");
    WindowedValue<String> secondElement = WindowedValue.valueInGlobalWindow("second");

    SideInputElement firstSideinput = new SideInputElement<>(0, ImmutableList.of("1"));
    SideInputElement secondSideinput = new SideInputElement(1, ImmutableList.of("2"));

    final Map<Integer, PCollectionView<?>> sideInputMap = new HashMap<>();
    sideInputMap.put(firstSideinput.getSideInputIndex(), view1);
    sideInputMap.put(secondSideinput.getSideInputIndex(), view2);
    final PushBackDoFnTransform<String, String> doFnTransform =
      new PushBackDoFnTransform(
        new SimpleSideInputDoFn<String>(view1, view2),
        NULL_INPUT_CODER,
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(),
        WindowingStrategy.globalDefault(),
        sideInputMap, /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        DisplayData.none());

    final TestOutputCollector<String> oc = new TestOutputCollector<>();
    doFnTransform.prepare(context, oc);

    // Main input first, Side inputs later
    doFnTransform.onData(firstElement);

    doFnTransform.onData(WindowedValue.valueInGlobalWindow(firstSideinput));
    doFnTransform.onData(WindowedValue.valueInGlobalWindow(secondSideinput));
    assertEquals(
      WindowedValue.valueInGlobalWindow(
        concat(firstElement.getValue(), firstSideinput.getSideInputValue(), secondSideinput.getSideInputValue())),
      oc.getOutput().get(0));

    // Side inputs first, Main input later
    doFnTransform.onData(secondElement);
    assertEquals(
      WindowedValue.valueInGlobalWindow(
        concat(secondElement.getValue(), firstSideinput.getSideInputValue(), secondSideinput.getSideInputValue())),
      oc.getOutput().get(1));

    // There should be only 2 final outputs
    assertEquals(2, oc.getOutput().size());

    // The side inputs should be "READY"
    assertTrue(doFnTransform.getSideInputReader().isReady(view1, GlobalWindow.INSTANCE));
    assertTrue(doFnTransform.getSideInputReader().isReady(view2, GlobalWindow.INSTANCE));

    // This watermark should remove the side inputs. (Now should be "NOT READY")
    doFnTransform.onWatermark(new Watermark(GlobalWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    Iterable materializedSideInput1 = doFnTransform.getSideInputReader().get(view1, GlobalWindow.INSTANCE);
    Iterable materializedSideInput2 = doFnTransform.getSideInputReader().get(view2, GlobalWindow.INSTANCE);
    assertFalse(materializedSideInput1.iterator().hasNext());
    assertFalse(materializedSideInput2.iterator().hasNext());

    // There should be only 2 final outputs
    doFnTransform.close();
    assertEquals(2, oc.getOutput().size());
  }


  /**
   * Bundle test do fn.
   */
  private static class BundleTestDoFn extends DoFn<String, String> {
    int count;

    private final List<Integer> bundleOutput;

    BundleTestDoFn(final List<Integer> bundleOutput) {
      this.bundleOutput = bundleOutput;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      count += 1;
      c.output(c.element());
    }

    @StartBundle
    public void startBundle(final StartBundleContext c) {
      count = 0;
    }

    @FinishBundle
    public void finishBundle(final FinishBundleContext c) {
      bundleOutput.add(count);
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
  private static class SimpleSideInputDoFn<T> extends DoFn<T, String> {
    private final PCollectionView<?> view1;
    private final PCollectionView<?> view2;

    public SimpleSideInputDoFn(final PCollectionView<?> view1,
                               final PCollectionView<?> view2) {
      this.view1 = view1;
      this.view2 = view2;
    }

    @ProcessElement
    public void processElement(final ProcessContext c) throws Exception {
      final T element = c.element();
      final Object view1Value = c.sideInput(view1);
      final Object view2Value = c.sideInput(view2);

      c.output(concat(element, view1Value, view2Value));
    }
  }

  private static String concat(final Object obj1, final Object obj2, final Object obj3) {
    return obj1.toString() + " / " + obj2 + " / " + obj3;
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
