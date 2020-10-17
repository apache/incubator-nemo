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

import com.google.common.collect.Iterables;
import junit.framework.TestCase;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

import static org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing.*;
import static org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES;
import static org.mockito.Mockito.mock;

public class GBKTransformTest extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(GBKTransformTest.class.getName());
  private final static Coder STRING_CODER = StringUtf8Coder.of();
  private final static Coder INTEGER_CODER = BigEndianIntegerCoder.of();

  private void checkOutput(final KV<String, Integer> expected, final KV<String, Integer> result) {
    // check key
    assertEquals(expected.getKey(), result.getKey());
    // check value
    assertEquals(expected.getValue(), result.getValue());
  }

  private void checkOutput2(final KV<String, List<String>> expected, final KV<String, Iterable<String>> result) {
    // check key
    assertEquals(expected.getKey(), result.getKey());
    // check value
    final List<String> resultValue = new ArrayList<>();
    final List<String> expectedValue = new ArrayList<>(expected.getValue());
    result.getValue().iterator().forEachRemaining(resultValue::add);
    Collections.sort(resultValue);
    Collections.sort(expectedValue);
    assertEquals(expectedValue, resultValue);
  }


  // Test Combine.Perkey operation.

  // Define combine function.
  public static class CountFn extends Combine.CombineFn<Integer, CountFn.Accum, Integer> {

    public static class Accum {
      int sum = 0;
    }

    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Integer input) {
      accum.sum += input;
      return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
      Accum merged = createAccumulator();
      for (Accum accum : accums) {
        merged.sum += accum.sum;
      }
      return merged;
    }

    @Override
    public Integer extractOutput(Accum accum) {
      return accum.sum;
    }

    @Override
    public Coder<CountFn.Accum> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputcoder) {
      return AvroCoder.of(CountFn.Accum.class);
    }
  }

  public final static Combine.CombineFn combine_fn = new CountFn();

  // window size: 10 sec
  // period: 5 sec
  //
  // [----------------------- window1 --------------------------]
  //                             [---------------------------window2-------------------------]
  //                                                            [-------------------------window3----------------------]
  //
  //  ts1 -- ts2 -------------------- ts3 -- w1 -- ts4 --------- ts5 - w2 ------------ ts6 -----ts7 -- w3 -- ts8 --ts9 --- w4

  // Test without late data
  @Test
  @SuppressWarnings("unchecked")
  public void test_combine() {
    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final SlidingWindows slidingWindows = SlidingWindows.of(Duration.standardSeconds(10))
      .every(Duration.standardSeconds(5));

    final Instant ts1 = new Instant(1000);
    final Instant ts2 = new Instant(2000);
    final Instant ts3 = new Instant(6000);
    final Instant ts4 = new Instant(8000);
    final Instant ts5 = new Instant(11000);
    final Instant ts6 = new Instant(14000);
    final Instant ts7 = new Instant(16000);
    final Instant ts8 = new Instant(17000);
    final Instant ts9 = new Instant(19000);
    final Watermark watermark1 = new Watermark(7000);
    final Watermark watermark2 = new Watermark(12000);
    final Watermark watermark3 = new Watermark(18000);
    final Watermark watermark4 = new Watermark(21000);

    AppliedCombineFn<String, Integer, CountFn.Accum, Integer> applied_combine_fn =
      AppliedCombineFn.withInputCoder(
        combine_fn,
        CoderRegistry.createDefault(),
        KvCoder.of(STRING_CODER, INTEGER_CODER),
        null,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES)
      );

    final GBKTransform<String, Integer, Integer> combine_transform =
      new GBKTransform(
        KvCoder.of(STRING_CODER, INTEGER_CODER),
        Collections.singletonMap(outputTag, KvCoder.of(STRING_CODER, INTEGER_CODER)),
        outputTag,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.combining(STRING_CODER, applied_combine_fn),
        DoFnSchemaInformation.create(),
        DisplayData.none(),
        false);

    // window1 : [-5000, 5000) in millisecond
    // window2 : [0, 10000)
    // window3 : [5000, 15000)
    // window4 : [10000, 20000)
    List<IntervalWindow> sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts1));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);
    final IntervalWindow window1 = sortedWindows.get(0);
    final IntervalWindow window2 = sortedWindows.get(1);
    sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts5));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);
    final IntervalWindow window3 = sortedWindows.get(0);
    final IntervalWindow window4 = sortedWindows.get(1);

    // Prepare to test CombineStreamTransform
    final Transform.Context context = mock(Transform.Context.class);
    final TestOutputCollector<KV<String, Integer>> oc = new TestOutputCollector();
    combine_transform.prepare(context, oc);

    combine_transform.onData(WindowedValue.of(
      KV.of("a", 1), ts1, slidingWindows.assignWindows(ts1), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("c", 1), ts2, slidingWindows.assignWindows(ts2), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("b", 1), ts3, slidingWindows.assignWindows(ts3), PaneInfo.NO_FIRING));

    // Emit outputs of window1
    combine_transform.onWatermark(watermark1);
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // Check outputs
    assertEquals(Arrays.asList(window1), oc.outputs.get(0).getWindows());
    assertEquals(2, oc.outputs.size());
    checkOutput(KV.of("a", 1), oc.outputs.get(0).getValue());
    checkOutput(KV.of("c", 1), oc.outputs.get(1).getValue());
    oc.outputs.clear();
    oc.watermarks.clear();

    combine_transform.onData(WindowedValue.of(
      KV.of("a", 1), ts4, slidingWindows.assignWindows(ts4), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("c", 1), ts5, slidingWindows.assignWindows(ts5), PaneInfo.NO_FIRING));

    // Emit outputs of window2
    combine_transform.onWatermark(watermark2);
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // Check outputs
    assertEquals(Arrays.asList(window2), oc.outputs.get(0).getWindows());
    assertEquals(3, oc.outputs.size());
    checkOutput(KV.of("a", 2), oc.outputs.get(0).getValue());
    checkOutput(KV.of("b", 1), oc.outputs.get(1).getValue());
    checkOutput(KV.of("c", 1), oc.outputs.get(2).getValue());
    oc.outputs.clear();
    oc.watermarks.clear();

    combine_transform.onData(WindowedValue.of(
      KV.of("b", 1), ts6, slidingWindows.assignWindows(ts6), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("b", 1), ts7, slidingWindows.assignWindows(ts7), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("a", 1), ts8, slidingWindows.assignWindows(ts8), PaneInfo.NO_FIRING));

    // Emit outputs of window3
    combine_transform.onWatermark(watermark3);
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // Check outputs
    assertEquals(Arrays.asList(window3), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 1), oc.outputs.get(0).getValue());
    checkOutput(KV.of("b", 2), oc.outputs.get(1).getValue());
    checkOutput(KV.of("c", 1), oc.outputs.get(2).getValue());
    oc.outputs.clear();
    oc.watermarks.clear();


    combine_transform.onData(WindowedValue.of(
      KV.of("c", 3), ts9, slidingWindows.assignWindows(ts9), PaneInfo.NO_FIRING));

    // Emit outputs of window3
    combine_transform.onWatermark(watermark4);
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // Check outputs
    assertEquals(Arrays.asList(window4), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 1), oc.outputs.get(0).getValue());
    checkOutput(KV.of("b", 2), oc.outputs.get(1).getValue());
    checkOutput(KV.of("c", 4), oc.outputs.get(2).getValue());

    oc.outputs.clear();
    oc.watermarks.clear();
  }

  // Test with late data
  @Test
  @SuppressWarnings("unchecked")
  public void test_combine_lateData() {
    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final Duration lateness = Duration.standardSeconds(2);
    final SlidingWindows slidingWindows = SlidingWindows.of(Duration.standardSeconds(10))
      .every(Duration.standardSeconds(5));

    final Instant ts1 = new Instant(1000);
    final Instant ts2 = new Instant(2000);
    final Instant ts3 = new Instant(4500);
    final Instant ts4 = new Instant(11000);
    final Watermark watermark1 = new Watermark(6500);
    final Watermark watermark2 = new Watermark(8000);

    AppliedCombineFn<String, Integer, CountFn.Accum, Integer> applied_combine_fn =
      AppliedCombineFn.withInputCoder(
        combine_fn,
        CoderRegistry.createDefault(),
        KvCoder.of(STRING_CODER, INTEGER_CODER),
        null,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES).withAllowedLateness(lateness)
      );

    final GBKTransform<String, Integer, Integer> combine_transform =
      new GBKTransform(
        KvCoder.of(STRING_CODER, INTEGER_CODER),
        Collections.singletonMap(outputTag, KvCoder.of(STRING_CODER, INTEGER_CODER)),
        outputTag,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES).withAllowedLateness(lateness),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.combining(STRING_CODER, applied_combine_fn),
        DoFnSchemaInformation.create(),
        DisplayData.none(),
        false);

    // window1 : [-5000, 5000) in millisecond
    // window2 : [0, 10000)
    // window3 : [5000, 15000)
    // window4 : [10000, 20000)
    List<IntervalWindow> sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts1));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);
    final IntervalWindow window1 = sortedWindows.get(0);
    final IntervalWindow window2 = sortedWindows.get(1);
    sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts4));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);
    final IntervalWindow window3 = sortedWindows.get(0);
    final IntervalWindow window4 = sortedWindows.get(1);

    // Prepare to test
    final Transform.Context context = mock(Transform.Context.class);
    final TestOutputCollector<KV<String, Integer>> oc = new TestOutputCollector();
    combine_transform.prepare(context, oc);

    combine_transform.onData(WindowedValue.of(
      KV.of("a", 1), ts1, slidingWindows.assignWindows(ts1), PaneInfo.NO_FIRING));
    combine_transform.onData(WindowedValue.of(
      KV.of("b", 1), ts2, slidingWindows.assignWindows(ts2), PaneInfo.NO_FIRING));

    // On-time firing of window1. Skipping checking outputs since test1 checks output from non-late data
    combine_transform.onWatermark(watermark1);
    oc.outputs.clear();

    // Late data in window 1. Should be accumulated since EOW + allowed lateness > current Watermark
    combine_transform.onData(WindowedValue.of(
      KV.of("a", 5), ts1, slidingWindows.assignWindows(ts1), PaneInfo.NO_FIRING));

    // Check outputs
    assertEquals(Arrays.asList(window1), oc.outputs.get(0).getWindows());
    assertEquals(1,oc.outputs.size());
    assertEquals(LATE, oc.outputs.get(0).getPane().getTiming());
    checkOutput(KV.of("a", 6), oc.outputs.get(0).getValue());

    oc.outputs.clear();
    oc.watermarks.clear();

    // Late data in window 1. Should NOT be accumulated to outputs of window1 since EOW + allowed lateness > current Watermark
    combine_transform.onWatermark(watermark2);
    combine_transform.onData(WindowedValue.of(
      KV.of("a", 10), ts3, slidingWindows.assignWindows(ts3), PaneInfo.NO_FIRING));
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));


    // Check outputs
    assertEquals(Arrays.asList(window1), oc.outputs.get(0).getWindows());
    assertEquals(1, oc.outputs.size());
    assertEquals(LATE, oc.outputs.get(0).getPane().getTiming());
    checkOutput(KV.of("a", 10), oc.outputs.get(0).getValue());
    oc.outputs.clear();
    oc.watermarks.clear();
  }

  // Now testing GroupbyKey Operation.

  // window size: 2 sec
  // interval size: 1 sec
  //
  //                           [--------------window2------------------------------]
  // [----------------------- window1 --------------------------]
  // [-------window0-------]
  // ts1 -- ts2 -- ts3 -- w -- ts4 -- w2 -- ts5 --ts6 --ts7 -- w3 -- ts8 --ts9 - --w4
  // (1, "hello")
  //      (1, "world")
  //             (2, "hello")
  //                   ==> window1: {(1,["hello","world"]), (2, ["hello"])}
  //                                 (1, "a")
  //                                                       (2,"a")
  //                                                             (3,"a")
  //                                                                  (2,"b")
  //                                                       => window2: {(1,"a"), (2,["a","b"]), (3,"a")}

  @Test
  @SuppressWarnings("unchecked")
  public void test_gbk() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final SlidingWindows slidingWindows = SlidingWindows.of(Duration.standardSeconds(2))
      .every(Duration.standardSeconds(1));

    final GBKTransform<String, String, Iterable<String>> doFnTransform =
      new GBKTransform(
        KvCoder.of(STRING_CODER, STRING_CODER),
        Collections.singletonMap(outputTag, KvCoder.of(STRING_CODER, IterableCoder.of(STRING_CODER))),
        outputTag,
        WindowingStrategy.of(slidingWindows),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.buffering(STRING_CODER),
        DoFnSchemaInformation.create(),
        DisplayData.none(),
        false);

    final Instant ts1 = new Instant(1);
    final Instant ts2 = new Instant(100);
    final Instant ts3 = new Instant(300);
    final Watermark watermark = new Watermark(1003);
    final Instant ts4 = new Instant(1200);
    final Watermark watermark2 = new Watermark(1400);
    final Instant ts5 = new Instant(1600);
    final Instant ts6 = new Instant(1800);
    final Instant ts7 = new Instant(1900);
    final Watermark watermark3 = new Watermark(2100);
    final Instant ts8 = new Instant(2200);
    final Instant ts9 = new Instant(2300);
    final Watermark watermark4 = new Watermark(3000);


    List<IntervalWindow> sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts1));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);

    // [0---1000)
    final IntervalWindow window0 = sortedWindows.get(0);
    // [0---2000)
    final IntervalWindow window1 = sortedWindows.get(1);

    sortedWindows.clear();
    sortedWindows = new ArrayList<>(slidingWindows.assignWindows(ts4));
    Collections.sort(sortedWindows, IntervalWindow::compareTo);

    // [1000--3000)
    final IntervalWindow window2 = sortedWindows.get(1);


    final Transform.Context context = mock(Transform.Context.class);
    final TestOutputCollector<KV<String, Iterable<String>>> oc = new TestOutputCollector();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "hello"), ts1, slidingWindows.assignWindows(ts1), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "world"), ts2, slidingWindows.assignWindows(ts2), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("2", "hello"), ts3, slidingWindows.assignWindows(ts3), PaneInfo.NO_FIRING));

    doFnTransform.onWatermark(watermark);

    // output
    // 1: ["hello", "world"]
    // 2: ["hello"]
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // windowed result for key 1
    assertEquals(Arrays.asList(window0), oc.outputs.get(0).getWindows());
    checkOutput2(KV.of("1", Arrays.asList("hello", "world")), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(window0), oc.outputs.get(1).getWindows());
    checkOutput2(KV.of("2", Arrays.asList("hello")), oc.outputs.get(1).getValue());

    assertEquals(2, oc.outputs.size());
    assertEquals(2, oc.watermarks.size());

    // check output watermark
    assertEquals(1000,
      oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "a"), ts4, slidingWindows.assignWindows(ts4), PaneInfo.NO_FIRING));


    doFnTransform.onWatermark(watermark2);

    assertEquals(0, oc.outputs.size()); // do not emit anything
    assertEquals(0, oc.watermarks.size());

    doFnTransform.onData(WindowedValue.of(
      KV.of("3", "a"), ts5, slidingWindows.assignWindows(ts5), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("3", "a"), ts6, slidingWindows.assignWindows(ts6), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("3", "b"), ts7, slidingWindows.assignWindows(ts7), PaneInfo.NO_FIRING));

    // emit window1
    doFnTransform.onWatermark(watermark3);

    // output
    // 1: ["hello", "world", "a"]
    // 2: ["hello"]
    // 3: ["a", "a", "b"]
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));


    // windowed result for key 1
    assertEquals(Arrays.asList(window1), oc.outputs.get(0).getWindows());
    checkOutput2(KV.of("1", Arrays.asList("hello", "world", "a")), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(window1), oc.outputs.get(1).getWindows());
    checkOutput2(KV.of("2", Arrays.asList("hello")), oc.outputs.get(1).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(window1), oc.outputs.get(2).getWindows());
    checkOutput2(KV.of("3", Arrays.asList("a", "a", "b")), oc.outputs.get(2).getValue());

    // check output watermark
    assertEquals(2000,
      oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();


    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "a"), ts8, slidingWindows.assignWindows(ts8), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("3", "b"), ts9, slidingWindows.assignWindows(ts9), PaneInfo.NO_FIRING));

    // emit window2
    doFnTransform.onWatermark(watermark4);

    // output
    // 1: ["a", "a"]
    // 3: ["a", "a", "b", "b"]
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    assertEquals(2, oc.outputs.size());

    // windowed result for key 1
    assertEquals(Arrays.asList(window2), oc.outputs.get(0).getWindows());
    checkOutput2(KV.of("1", Arrays.asList("a", "a")), oc.outputs.get(0).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(window2), oc.outputs.get(1).getWindows());
    checkOutput2(KV.of("3", Arrays.asList("a", "a", "b", "b")), oc.outputs.get(1).getValue());

    // check output watermark
    assertEquals(3000,
      oc.watermarks.get(0).getTimestamp());

    doFnTransform.close();
  }

  /**
   * Test complex triggers that emit early and late firing.
   */
  @Test
  public void test_gbk_eventTimeTrigger() {
    final Duration lateness = Duration.standardSeconds(1);
    final AfterWatermark.AfterWatermarkEarlyAndLate trigger = AfterWatermark.pastEndOfWindow()
      // early firing
      .withEarlyFirings(
        AfterProcessingTime
          .pastFirstElementInPane()
          // early firing 1 sec after receiving an element
          .plusDelayOf(Duration.millis(1000)))
      // late firing: Fire on any late data.
      .withLateFirings(AfterPane.elementCountAtLeast(1));

    final FixedWindows window = (FixedWindows) Window.into(
      FixedWindows.of(Duration.standardSeconds(5)))
      // lateness
      .withAllowedLateness(lateness)
      .triggering(trigger)
      // TODO #308: Test discarding of refinements
      .accumulatingFiredPanes().getWindowFn();

    final TupleTag<String> outputTag = new TupleTag<>("main-output");

    final GBKTransform<String, String, Iterable<String>> doFnTransform =
      new GBKTransform(
        KvCoder.of(STRING_CODER, STRING_CODER),
        Collections.singletonMap(outputTag, KvCoder.of(STRING_CODER, IterableCoder.of(STRING_CODER))),
        outputTag,
        WindowingStrategy.of(window).withTrigger(trigger)
          .withMode(ACCUMULATING_FIRED_PANES)
          .withAllowedLateness(lateness),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.buffering(STRING_CODER),
        DoFnSchemaInformation.create(),
        DisplayData.none(),
        false);


    final Transform.Context context = mock(Transform.Context.class);
    final TestOutputCollector<KV<String, Iterable<String>>> oc = new TestOutputCollector();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "hello"), new Instant(1), window.assignWindow(new Instant(1)), PaneInfo.NO_FIRING));

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // early firing is not related to the watermark progress
    doFnTransform.onWatermark(new Watermark(2));
    assertEquals(1, oc.outputs.size());
    assertEquals(EARLY, oc.outputs.get(0).getPane().getTiming());
    oc.outputs.clear();

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "world"), new Instant(3), window.assignWindow(new Instant(3)), PaneInfo.NO_FIRING));
    // EARLY firing... waiting >= 1 sec
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // GBKTransform emits data when receiving watermark
    // TODO #250: element-wise processing
    doFnTransform.onWatermark(new Watermark(5));
    assertEquals(1, oc.outputs.size());
    assertEquals(EARLY, oc.outputs.get(0).getPane().getTiming());
    // ACCUMULATION MODE
    checkOutput2(KV.of("1", Arrays.asList("hello", "world")), oc.outputs.get(0).getValue());
    oc.outputs.clear();

    // ON TIME
    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "!!"), new Instant(3), window.assignWindow(new Instant(3)), PaneInfo.NO_FIRING));
    doFnTransform.onWatermark(new Watermark(5001));
    assertEquals(1, oc.outputs.size());
    assertEquals(ON_TIME, oc.outputs.get(0).getPane().getTiming());
    // ACCUMULATION MODE
    checkOutput2(KV.of("1", Arrays.asList("hello", "world", "!!")), oc.outputs.get(0).getValue());
    oc.outputs.clear();

    // LATE DATA
    // actual window: [0-5000)
    // allowed lateness: 1000 (ms)
    // current watermark: 5001
    // data: 1000
    // End of current window + allowed lateness >= current watermark (4999 + 1000 >= 5001)
    // so it should be accumulated to the prev window
    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "bye!"), new Instant(1000),
      window.assignWindow(new Instant(1000)), PaneInfo.NO_FIRING));
    doFnTransform.onWatermark(new Watermark(6000));
    assertEquals(1, oc.outputs.size());
    assertEquals(LATE, oc.outputs.get(0).getPane().getTiming());
    // The data should  be accumulated to the previous window because it allows 1 second lateness
    checkOutput2(KV.of("1", Arrays.asList("hello", "world", "!!", "bye!")), oc.outputs.get(0).getValue());
    oc.outputs.clear();

    // LATE DATA
    // actual window: [0-5000)
    // allowed lateness: 1000 (ms)
    // data timestamp: 4800
    // current watermark: 6000
    // End of current window + allowed lateness < current watermark (4999 + 1000 < 6000)
    // It should not be accumulated to the prev window
    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "hello again!"), new Instant(4800),
      window.assignWindow(new Instant(4800)), PaneInfo.NO_FIRING));
    doFnTransform.onWatermark(new Watermark(6300));
    assertEquals(1, oc.outputs.size());
    assertEquals(LATE, oc.outputs.get(0).getPane().getTiming());
    checkOutput2(KV.of("1", Arrays.asList("hello again!")), oc.outputs.get(0).getValue());
    oc.outputs.clear();
    doFnTransform.close();
  }
}
