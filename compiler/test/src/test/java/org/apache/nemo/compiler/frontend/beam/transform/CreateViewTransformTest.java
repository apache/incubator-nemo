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
import org.apache.beam.sdk.transforms.Materialization;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
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

public final class CreateViewTransformTest {

  private final static Coder NULL_INPUT_CODER = null;
  private final static Map<TupleTag<?>, Coder<?>> NULL_OUTPUT_CODERS = null;

  // [---- window1 --------]         [--------------- window2 ---------------]
  // ts1 -- ts2 -- ts3 -- watermark -- ts4 -- watermark2 -- ts5 --ts6 --ts7 -- watermark7
  // (null, "hello")
  //      (null, "world")
  //             (null, "hello")
  //                   ==> window1: {3} (calculate # of elements)
  //                                 (null, "a")
  //                                                       (null,"a")
  //                                                             (null,"a")
  //                                                                  (null,"b")
  //                                                       => window2: {4} (calculate # of elements)
  @Test
  @SuppressWarnings("unchecked")
  public void test() {

    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final FixedWindows fixedwindows = FixedWindows.of(Duration.standardSeconds(1));
    final GroupByKeyAndWindowDoFnTransform<String, String> gbkTransform =
      new GroupByKeyAndWindowDoFnTransform(
        NULL_OUTPUT_CODERS,
        outputTag,
        Collections.emptyList(), /* additional outputs */
        WindowingStrategy.of(fixedwindows),
        emptyList(), /* side inputs */
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.buffering(NULL_INPUT_CODER));
    final CreateViewTransform<String, Integer> viewTransform =
      new CreateViewTransform(new SumViewFn());

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
    final TestOutputCollector<Integer> oc = new TestOutputCollector();
    viewTransform.prepare(context, oc);

    viewTransform.onData(WindowedValue.of(
      KV.of(null, "hello"), ts1, fixedwindows.assignWindow(ts1), PaneInfo.NO_FIRING));

    viewTransform.onData(WindowedValue.of(
      KV.of(null, "world"), ts2, fixedwindows.assignWindow(ts2), PaneInfo.NO_FIRING));

    viewTransform.onData(WindowedValue.of(
      KV.of(null, "hello"), ts3, fixedwindows.assignWindow(ts3), PaneInfo.NO_FIRING));

    viewTransform.onWatermark(watermark);

    // materialized data
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts1)), oc.outputs.get(0).getWindows());
    assertEquals(new Integer(3), oc.outputs.get(0).getValue());

    // check output watermark
    assertEquals(fixedwindows.assignWindow(ts1).maxTimestamp().getMillis(),
      oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();


    viewTransform.onData(WindowedValue.of(
      KV.of(null, "a"), ts4, fixedwindows.assignWindow(ts4), PaneInfo.NO_FIRING));

    // do not emit anything
    viewTransform.onWatermark(watermark2);
    assertEquals(0, oc.outputs.size());
    assertEquals(0, oc.watermarks.size());

    viewTransform.onData(WindowedValue.of(
      KV.of(null, "a"), ts5, fixedwindows.assignWindow(ts5), PaneInfo.NO_FIRING));

    viewTransform.onData(WindowedValue.of(
      KV.of(null, "a"), ts6, fixedwindows.assignWindow(ts6), PaneInfo.NO_FIRING));

    viewTransform.onData(WindowedValue.of(
      KV.of(null, "b"), ts7, fixedwindows.assignWindow(ts7), PaneInfo.NO_FIRING));

    // emit windowed value
    viewTransform.onWatermark(watermark3);

    // materialized data
    assertEquals(Arrays.asList(fixedwindows.assignWindow(ts4)), oc.outputs.get(0).getWindows());
    assertEquals(new Integer(4), oc.outputs.get(0).getValue());

    // check output watermark
    assertEquals(fixedwindows.assignWindow(ts4).maxTimestamp().getMillis(),
      oc.watermarks.get(0).getTimestamp());

    viewTransform.close();
  }

  final class SumViewFn extends ViewFn<Materializations.MultimapView<Void, String>, Integer> {

    @Override
    public Materialization<Materializations.MultimapView<Void, String>> getMaterialization() {
      return null;
    }

    @Override
    public Integer apply(final Materializations.MultimapView<Void, String> view) {
      int sum = 0;
      for (String s : view.get(null)) {
        sum += 1;
      }
      return sum;
    }
  }
}
