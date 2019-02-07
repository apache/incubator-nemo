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

import com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.NemoUtils;
import org.apache.nemo.common.OffloadingWorker;
import org.apache.nemo.common.OffloadingWorkerFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.lambda.Constants;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;
import org.apache.nemo.compiler.frontend.beam.coder.BeamDecoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.BeamEncoderFactory;
import org.apache.nemo.compiler.frontend.beam.coder.PushBackCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * DoFn transform implementation with push backs for side inputs.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class PushBackDoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(PushBackDoFnTransform.class.getName());

  private List<WindowedValue<InputT>> curPushedBacks;
  private long curPushedBackWatermark; // Long.MAX_VALUE when no pushed-back exists.
  private long curInputWatermark;
  private long curOutputWatermark;

  private boolean offloading = true; // TODO: fix

  private PushBackLambdaDoFnTransform offloadingTransform;
  private final List<String> serializedVertices;

  private final Coder mainCoder;
  private final Coder sideCoder;

  /**
   * PushBackDoFnTransform Constructor.
   */
  public PushBackDoFnTransform(final DoFn<InputT, OutputT> doFn,
                               final Coder<InputT> inputCoder,
                               final Map<TupleTag<?>, Coder<?>> outputCoders,
                               final TupleTag<OutputT> mainOutputTag,
                               final List<TupleTag<?>> additionalOutputTags,
                               final WindowingStrategy<?, ?> windowingStrategy,
                               final Map<Integer, PCollectionView<?>> sideInputs,
                               final PipelineOptions options,
                               final DisplayData displayData,
                               final Coder mainCoder,
                               final Coder sideCoder) {
    super(doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options, displayData);
    this.curPushedBacks = new ArrayList<>();
    this.curPushedBackWatermark = Long.MAX_VALUE;
    this.curInputWatermark = Long.MIN_VALUE;
    this.curOutputWatermark = Long.MIN_VALUE;
    this.offloadingTransform = new PushBackLambdaDoFnTransform(
      doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options, displayData);
    this.serializedVertices = Arrays.asList(NemoUtils.serializeToString(offloadingTransform));
    this.mainCoder = mainCoder;
    this.sideCoder = sideCoder;
  }

  @Override
  protected DoFn wrapDoFn(final DoFn initDoFn) {
    return initDoFn;
  }

  public void setOffloading(boolean offload) {
    offloading = offload;
  }

  @Override
  public void onData(final WindowedValue data) {
    // Need to distinguish side/main inputs and push-back main inputs.
    if (data.getValue() instanceof SideInputElement) {
      // This element is a Side Input
      final long st = System.currentTimeMillis();
      LOG.info("Receive Side input at {}: {}", this.hashCode(), data);
      System.out.println("Side input start time: " + System.currentTimeMillis() + " " + data);
      //System.out.println("Receive Side input at " + data);
      // TODO #287: Consider Explicit Multi-Input IR Transform
      final WindowedValue<SideInputElement> sideInputElement = (WindowedValue<SideInputElement>) data;
      final PCollectionView view = getSideInputs().get(sideInputElement.getValue().getSideInputIndex());
      getSideInputReader().addSideInputElement(view, data);

      //LOG.info("{}, Add side input at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);

      int cnt = handlePushBacksWithSideInput(sideInputElement);
      LOG.info("{}, Handle pushback cnt: {} at {}: {}", System.currentTimeMillis() - st,
        cnt, this.hashCode(), data);
      //System.out.println("{}, Handle pushback cnt: " + cnt + " data : " + data);

      // See if we can emit a new watermark, as we may have processed some pushed-back elements
      onWatermark(new Watermark(curInputWatermark));
      //LOG.info("{}, On watermark at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);
    } else {
      // This element is the Main Input
      checkAndInvokeBundle();
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(data);
      int cnt = 0;
      for (final WindowedValue wv : pushedBack) {
        cnt += 1;
        curPushedBackWatermark = Math.min(curPushedBackWatermark, wv.getTimestamp().getMillis());
        curPushedBacks.add(wv);
      }
      //System.out.println("pushback count: " + cnt);
      checkAndFinishBundle();
    }
  }


  private int handlePushBacksWithSideInput(final WindowedValue<SideInputElement> sideInput) {
    // Force-finish, before (possibly) processing pushed-back data.
    //
    // Main reason:
    // {@link org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner}
    // caches for each bundle the side inputs that are not ready.
    // We need to re-start the bundle to advertise the (possibly) newly available side input.
    forceFinishBundle(); // forced

    // With the new side input added, we may be able to process some pushed-back elements.


    /*
    checkAndInvokeBundle();
    final int cnt = curPushedBacks.size();
    final List<Pair<List<WindowedValue<InputT>>, Long>> result =
      burstyOutputCollector.burstyOperation(curPushedBacks,
        (windowedValues) -> {
          long pushedBackAgainWatermark = Long.MAX_VALUE;
          final List<WindowedValue<InputT>> pushedBackAgain = new ArrayList<>();
          for (final WindowedValue<InputT> curPushedBack : windowedValues) {
            final Iterable<WindowedValue<InputT>> pushedBack =
              getPushBackRunner().processElementInReadyWindows(curPushedBack);
            for (final WindowedValue<InputT> wv : pushedBack) {
              pushedBackAgainWatermark = Math.min(pushedBackAgainWatermark, wv.getTimestamp().getMillis());
              pushedBackAgain.add(wv);
            }
          }
          return Pair.of(pushedBackAgain, pushedBackAgainWatermark);
        }
      );

    checkAndFinishBundle();
    */

    checkAndInvokeBundle();
    long pushedBackAgainWatermark = Long.MAX_VALUE;
    final List<WindowedValue<InputT>> pushedBackAgain = new LinkedList<>();
    int cnt = 0;

    for (final WindowedValue<InputT> curPushedBack : curPushedBacks) {
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(curPushedBack);
      cnt += 1;

      for (final WindowedValue<InputT> wv : pushedBack) {
        pushedBackAgainWatermark = Math.min(pushedBackAgainWatermark, wv.getTimestamp().getMillis());
        pushedBackAgain.add(wv);
      }

      // TODO: adhoc solution
      if (offloading) {
        break;
      }
    }

    if (offloading) {
      LOG.info("Start to offloading");

      // partition
      final ExecutorService executorService = Executors.newCachedThreadPool();
      final int plusOne = (curPushedBacks.size() - cnt) % Constants.POOL_SIZE > 0 ? 1 : 0;
      final int partitionSize = ((curPushedBacks.size() - cnt) / Constants.POOL_SIZE) + plusOne;
      final List<List<WindowedValue<InputT>>> partitions = Lists.partition(
        curPushedBacks.subList(cnt, curPushedBacks.size()), partitionSize);
      final OffloadingWorkerFactory offloadingWorkerFactory = getContext().getOffloadingWorkerFactory();

      LOG.info("# of partition: {}, partitionSize: {}", partitions.size(), partitionSize);
      final List<Future<List<WindowedValue<OutputT>>>> results = new ArrayList<>(partitions.size());

      for (final List<WindowedValue<InputT>> partition : partitions) {
        results.add(executorService.submit(() -> {
        final OffloadingWorker worker = offloadingWorkerFactory.createOffloadingWorker(serializedVertices,
          new BeamEncoderFactory(new PushBackCoder(sideCoder, mainCoder)),
          new BeamDecoderFactory(new PushBackCoder(sideCoder, mainCoder)),
          new BeamEncoderFactory(mainCoder), new BeamDecoderFactory(mainCoder));

        worker.write(sideInput);
        for (final WindowedValue<InputT> val : partition) {
          worker.write(val);
        }

        worker.flush();
        worker.finishOffloading();
        return worker.getResult();
        }));
      }

      // waiting results
      for (final Future<List<WindowedValue<OutputT>>> future : results) {
        try {
          final List<WindowedValue<OutputT>> result = future.get();
          // Emit results
          result.stream().forEach(val -> {
            LOG.info("Result: {}", val);
            getOutputCollector().emit(val);
          });
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }


      // TODO: fix
      //offloading = false;
    }
    checkAndFinishBundle();

    // TODO: need to guarantee correctness
    curPushedBacks = pushedBackAgain;
    curPushedBackWatermark = pushedBackAgainWatermark;
    return cnt;
  }

  private int handlePushBacks() {
    // Force-finish, before (possibly) processing pushed-back data.
    //
    // Main reason:
    // {@link org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner}
    // caches for each bundle the side inputs that are not ready.
    // We need to re-start the bundle to advertise the (possibly) newly available side input.
    forceFinishBundle(); // forced

    // With the new side input added, we may be able to process some pushed-back elements.
    final List<WindowedValue<InputT>> pushedBackAgain = new LinkedList<>();
    long pushedBackAgainWatermark = Long.MAX_VALUE;
    int cnt = 0;
    for (final WindowedValue<InputT> curPushedBack : curPushedBacks) {
      checkAndInvokeBundle();
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(curPushedBack);
      cnt += 1;
      checkAndFinishBundle();
      for (final WindowedValue<InputT> wv : pushedBack) {
        pushedBackAgainWatermark = Math.min(pushedBackAgainWatermark, wv.getTimestamp().getMillis());
        pushedBackAgain.add(wv);
      }
    }

    curPushedBacks = pushedBackAgain;
    curPushedBackWatermark = pushedBackAgainWatermark;
    return cnt;
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    // TODO #298: Consider Processing DoFn PushBacks on Watermark
    checkAndInvokeBundle();
    curInputWatermark = watermark.getTimestamp();
    getSideInputReader().setCurrentWatermarkOfAllMainAndSideInputs(curInputWatermark);

    final long outputWatermarkCandidate = Math.min(curInputWatermark, curPushedBackWatermark);
    if (outputWatermarkCandidate > curOutputWatermark) {
      // Watermark advances!
      getOutputCollector().emitWatermark(new Watermark(outputWatermarkCandidate));
      curOutputWatermark = outputWatermarkCandidate;
    }
    checkAndFinishBundle();
  }

  @Override
  protected void beforeClose() {
    // This makes all unavailable side inputs as available empty side inputs.
    onWatermark(new Watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    // All push-backs should be processed here.
    handlePushBacks();
  }

  @Override
  OutputCollector wrapOutputCollector(final OutputCollector oc) {
    return oc;
  }
}
