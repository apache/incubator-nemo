package org.apache.nemo.compiler.frontend.beam.transform;

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
import static org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode.DISCARDING_FIRED_PANES;
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
  public void test1() {
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
        kv_coder,
        null,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES)
      );

    final GBKFinalTransform<String, Integer, Integer> combine_transform =
      new GBKFinalTransform(
        key_coder,
        null_coder,
        outputTag,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.combining(key_coder, applied_combine_fn),
        DoFnSchemaInformation.create(),
        DisplayData.none());

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
  public void test2() {
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
        kv_coder,
        null,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES).withAllowedLateness(lateness)
      );

    final GBKFinalTransform<String, Integer, Integer> combine_transform =
      new GBKFinalTransform(
        key_coder,
        null_coder,
        outputTag,
        WindowingStrategy.of(slidingWindows).withMode(ACCUMULATING_FIRED_PANES).withAllowedLateness(lateness),
        PipelineOptionsFactory.as(NemoPipelineOptions.class),
        SystemReduceFn.combining(key_coder, applied_combine_fn),
        DoFnSchemaInformation.create(),
        DisplayData.none());

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
}
