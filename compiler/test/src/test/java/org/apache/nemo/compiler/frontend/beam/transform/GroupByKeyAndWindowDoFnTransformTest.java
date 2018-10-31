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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.reef.io.Tuple;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.*;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

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

    assertEquals(expectedValue, resultValue);
  }


  // [---- window1 --------]         [--------------- window2 ---------------]
  // ts1 -- ts2 -- ts3 -- watermark -- ts4 -- watermark2 -- ts5 --ts6 --ts7 -- watermark7
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
    final FixedWindows fixedwindows = FixedWindows.of(Duration.standardSeconds(1));
    final GroupByKeyAndWindowDoFnTransform<String, String> doFnTransform =
      new GroupByKeyAndWindowDoFnTransform(
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(), /* additional outputs */
        WindowingStrategy.of(fixedwindows),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.buffering(NULL_INPUT_CODER));

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


    final Transform.Context context = mock(Transform.Context.class);
    final TestOutputCollector<KV<String, Iterable<String>>> oc = new TestOutputCollector();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "hello"), ts1, fixedwindows.assignWindow(ts1), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "world"), ts2, fixedwindows.assignWindow(ts2), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("2", "hello"), ts3, fixedwindows.assignWindow(ts3), PaneInfo.NO_FIRING));

    doFnTransform.onWatermark(watermark);

    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // windowed result for key 1
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts1)), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("1", Arrays.asList("hello", "world")), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts3)), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("2", Arrays.asList("hello")), oc.outputs.get(1).getValue());

    // check output watermark
    assertEquals(fixedwindows.assignWindow(ts1).maxTimestamp().getMillis(),
      oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();

    doFnTransform.onData(WindowedValue.of(
      KV.of("1", "a"), ts4, fixedwindows.assignWindow(ts4), PaneInfo.NO_FIRING));

    // do not emit anything
    doFnTransform.onWatermark(watermark2);
    assertEquals(0, oc.outputs.size());
    assertEquals(0, oc.watermarks.size());

    doFnTransform.onData(WindowedValue.of(
      KV.of("2", "a"), ts5, fixedwindows.assignWindow(ts5), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("3", "a"), ts6, fixedwindows.assignWindow(ts6), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("2", "b"), ts7, fixedwindows.assignWindow(ts7), PaneInfo.NO_FIRING));

    // emit windowed value
    doFnTransform.onWatermark(watermark3);

    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // windowed result for key 1
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts4)), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("1", Arrays.asList("a")), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts5)), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("2", Arrays.asList("a", "b")), oc.outputs.get(1).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts6)), oc.outputs.get(2).getWindows());
    checkOutput(KV.of("3", Arrays.asList("a")), oc.outputs.get(2).getValue());

    // check output watermark
    assertEquals(fixedwindows.assignWindow(ts4).maxTimestamp().getMillis(),
      oc.watermarks.get(0).getTimestamp());

    doFnTransform.close();
  }
}
