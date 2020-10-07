package org.apache.nemo.compiler.frontend.beam.transform;

import junit.framework.TestCase;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.util.AppliedCombineFn;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class GBKFinalTransformTest extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyAndWindowDoFnTransformTest.class.getName());
  private final static Coder key_coder = StringUtf8Coder.of();
  private final static Coder input_coder = BigEndianIntegerCoder.of();
  private final static KvCoder<String, Integer> kv_coder = KvCoder.of(key_coder, input_coder);
  private final static Map<TupleTag<?>, Coder<?>> null_coder = null;

  private void checkOutput(final KV<String, Integer> expected, final KV<String, Integer> result) {

    // check key
    assertEquals(expected.getKey(), result.getKey());

    // check value
    assertEquals(expected.getValue(), result.getValue());
  }

  // defining Combine function.
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

  @Test
  @SuppressWarnings("unchecked")
  public void test() {
    final TupleTag<String> outputTag = new TupleTag<>("main-output");
    final SlidingWindows slidingWindows = SlidingWindows.of(Duration.standardSeconds(2))
      .every(Duration.standardSeconds(1));

    AppliedCombineFn<String, Integer, CountFn.Accum, Integer> applied_combine_fn =
      AppliedCombineFn.withInputCoder(
        combine_fn,
        CoderRegistry.createDefault(),
        kv_coder,
        null,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES)
      );

    final GBKFinalTransform<String, Integer, Integer> doFnTransform =
      new GBKFinalTransform(
        key_coder,
        null_coder,
        outputTag,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.combining(key_coder, applied_combine_fn),
        DoFnSchemaInformation.create(),
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
    final TestOutputCollector<KV<String, Integer>> oc = new TestOutputCollector();
    doFnTransform.prepare(context, oc);

    doFnTransform.onData(WindowedValue.of(
      KV.of("a", 1), ts1, slidingWindows.assignWindows(ts1), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("a", 2), ts2, slidingWindows.assignWindows(ts2), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("b", 1), ts3, slidingWindows.assignWindows(ts3), PaneInfo.NO_FIRING));

    doFnTransform.onWatermark(watermark);

    // output
    // a: 3
    // b: 1
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    // windowed result for key 1
    assertEquals(Arrays.asList(window0), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 3), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(window0), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("b", 1), oc.outputs.get(1).getValue());

    assertEquals(2, oc.outputs.size());
    assertEquals(1, oc.watermarks.size());

    // check output watermark
    assertEquals(1000,
      oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();

    doFnTransform.onData(WindowedValue.of(
      KV.of("a", 3), ts4, slidingWindows.assignWindows(ts4), PaneInfo.NO_FIRING));


    doFnTransform.onWatermark(watermark2);

    assertEquals(0, oc.outputs.size()); // do not emit anything
    assertEquals(0, oc.watermarks.size());

    doFnTransform.onData(WindowedValue.of(
      KV.of("c", 1), ts5, slidingWindows.assignWindows(ts5), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("c", 2), ts6, slidingWindows.assignWindows(ts6), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("c", 3), ts7, slidingWindows.assignWindows(ts7), PaneInfo.NO_FIRING));

    // emit window1
    doFnTransform.onWatermark(watermark3);

    // output
    // a: 6
    // b: 1
    // c: 6
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));


    // windowed result for key 1
    assertEquals(Arrays.asList(window1), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 6), oc.outputs.get(0).getValue());

    // windowed result for key 2
    assertEquals(Arrays.asList(window1), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("b", 1), oc.outputs.get(1).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(window1), oc.outputs.get(2).getWindows());
    checkOutput(KV.of("c", 6), oc.outputs.get(2).getValue());

    // check output watermark
    //assertEquals(2000,
    //  oc.watermarks.get(0).getTimestamp());

    oc.outputs.clear();
    oc.watermarks.clear();


    doFnTransform.onData(WindowedValue.of(
      KV.of("a", 4), ts8, slidingWindows.assignWindows(ts8), PaneInfo.NO_FIRING));

    doFnTransform.onData(WindowedValue.of(
      KV.of("c", 4), ts9, slidingWindows.assignWindows(ts9), PaneInfo.NO_FIRING));

    // emit window2
    doFnTransform.onWatermark(watermark4);

    // output
    // a: 7
    // c: 10
    Collections.sort(oc.outputs, (o1, o2) -> o1.getValue().getKey().compareTo(o2.getValue().getKey()));

    assertEquals(2, oc.outputs.size());

    // windowed result for key 1
    assertEquals(Arrays.asList(window2), oc.outputs.get(0).getWindows());
    checkOutput(KV.of("a", 7), oc.outputs.get(0).getValue());

    // windowed result for key 3
    assertEquals(Arrays.asList(window2), oc.outputs.get(1).getWindows());
    checkOutput(KV.of("c", 10), oc.outputs.get(1).getValue());

    // check output watermark
    //assertEquals(3000,
      //oc.watermarks.get(0).getTimestamp());

    doFnTransform.close();
  }




}
