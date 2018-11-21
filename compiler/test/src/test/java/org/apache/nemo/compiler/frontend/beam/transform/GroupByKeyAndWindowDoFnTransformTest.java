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

import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.*;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

// TODO #270: Test different triggers
public final class GroupByKeyAndWindowDoFnTransformTest {

  private final static Coder NULL_INPUT_CODER = null;
  private final static Map<TupleTag<?>, Coder<?>> NULL_OUTPUT_CODERS = null;

  private void checkOutput(final KV<String, List<String>> expected, final KV<String, Iterable<String>> result) {

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
  public void test() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final SlidingWindows slidingWindows = SlidingWindows.of(Duration.standardSeconds(2))
      .every(Duration.standardSeconds(1));

    final GroupByKeyAndWindowDoFnTransform<String, String> doFnTransform =
      new GroupByKeyAndWindowDoFnTransform(
        NULL_OUTPUT_CODERS,
        outputTag,
        WindowingStrategy.of(slidingWindows),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.buffering(NULL_INPUT_CODER),
        DisplayData.none());

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
    checkOutput(KV.of("1", Arrays.asList("hello", "world")), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(window0), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("2", Arrays.asList("hello")), oc.outputs.get(1).getValue());

    assertEquals(2, oc.outputs.size());
    assertEquals(1, oc.watermarks.size());

    // check output watermark
    assertEquals(1000,
      oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "a"), ts4, slidingWindows.assignWindows(ts4), PaneInfo.NO_FIRING));


    doFnTransform.onWatermark(watermark2);

    assertEquals(0, oc.outputs.size()); // do not emit anything
   assertEquals(1, oc.watermarks.size());

    // check output watermark
    assertEquals(1400,
      oc.watermarks.get(0).getTimestamp());

    oc.watermarks.clear();

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
    checkOutput(KV.of("1", Arrays.asList("hello", "world", "a")), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(window1), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("2", Arrays.asList("hello")), oc.outputs.get(1).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(window1), oc.outputs.get(2).getWindows());
    checkOutput(KV.of("3", Arrays.asList("a", "a", "b")), oc.outputs.get(2).getValue());

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
    checkOutput(KV.of("1", Arrays.asList("a", "a")), oc.outputs.get(0).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(window2), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("3", Arrays.asList("a", "a", "b", "b")), oc.outputs.get(1).getValue());

    // check output watermark
    assertEquals(3000,
      oc.watermarks.get(0).getTimestamp());

    doFnTransform.close();
  }
}
