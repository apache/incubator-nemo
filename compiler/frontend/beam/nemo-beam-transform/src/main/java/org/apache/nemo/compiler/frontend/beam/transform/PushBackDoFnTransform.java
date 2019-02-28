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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.offloading.client.ServerlessExecutorProvider;
import org.apache.nemo.offloading.client.ServerlessExecutorService;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;
import org.apache.nemo.compiler.frontend.beam.coder.PushBackCoder2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * DoFn transform implementation with push backs for side inputs.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class PushBackDoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(PushBackDoFnTransform.class.getName());

  private transient List<WindowedValue<InputT>> curPushedBacks;
  private transient List<ByteBufOutputStream> byteBufList;

  private long curPushedBackWatermark; // Long.MAX_VALUE when no pushed-back exists.
  private long curInputWatermark;
  private long curOutputWatermark;

  private boolean offloading = false; // TODO: fix

  //private PushBackOffloadingTransform offloadingTransform;

  private final Coder mainCoder;
  private final Coder sideCoder;

  private boolean initialized = false;
  private transient EventHandler<WindowedValue<OutputT>> eventHandler;
  private transient OffloadingSerializer<
    Pair<WindowedValue<SideInputElement>, List<WindowedValue<InputT>>>, WindowedValue<OutputT>> offloadingSerializer;

  private ServerlessExecutorProvider slsProvider;

  final DoFn<InputT, OutputT> doFn;
  final Coder<InputT> inputCoder;
  final Map<TupleTag<?>, Coder<?>> outputCoders;
  final TupleTag<OutputT> mainOutputTag;
  final List<TupleTag<?>> additionalOutputTags;
  final WindowingStrategy<?, ?> windowingStrategy;
  final Map<Integer, PCollectionView<?>> sideInputs;
  final SerializablePipelineOptions options;
  final DisplayData displayData;

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
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.options = new SerializablePipelineOptions(options);
    this.displayData = displayData;

    this.curPushedBackWatermark = Long.MAX_VALUE;
    this.curInputWatermark = Long.MIN_VALUE;
    this.curOutputWatermark = Long.MIN_VALUE;
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
    if (!initialized) {
      byteBufList = new ArrayList<>();
      curPushedBacks = new ArrayList<>();

      eventHandler = new EventHandler<WindowedValue<OutputT>>() {
        @Override
        public void onNext(WindowedValue<OutputT> msg) {

          getOutputCollector().emit(msg);
        }
      };


      final OffloadingCoderWrapper<Pair<WindowedValue<SideInputElement>, List<WindowedValue<InputT>>>>
        inputCoder = new OffloadingCoderWrapper(new PushBackCoder2(sideCoder ,mainCoder));
      final OffloadingCoderWrapper outputCoder = new OffloadingCoderWrapper(mainCoder);

      offloadingSerializer =
        new OffloadingSerializer<Pair<WindowedValue<SideInputElement>, List<WindowedValue<InputT>>>, WindowedValue<OutputT>>() {
          @Override
          public OffloadingEncoder<Pair<WindowedValue<SideInputElement>, List<WindowedValue<InputT>>>> getInputEncoder() {
            return inputCoder;
          }
          @Override
          public OffloadingDecoder<Pair<WindowedValue<SideInputElement>, List<WindowedValue<InputT>>>> getInputDecoder() {
            return inputCoder;
          }
          @Override
          public OffloadingEncoder<WindowedValue<OutputT>> getOutputEncoder() {
            return outputCoder;
          }
          @Override
          public OffloadingDecoder<WindowedValue<OutputT>> getOutputDecoder() {
            return outputCoder;
          }
        };

      initialized = true;

      slsProvider = getContext().getServerlessExecutorProvider();
    }

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

      int cnt;
      if (offloading) {
        cnt = handlePushBacksWithSideInput(sideInputElement);
      } else {
        cnt = handlePushBacks();
      }

      System.out.println("Timestamp of pushback last " + System.currentTimeMillis());

      LOG.info("{}, Handle pushback cnt: {} at {}: {}", System.currentTimeMillis() - st,
        cnt, this.hashCode(), data);
      //System.out.println("{}, Handle pushback cnt: " + cnt + " data : " + data);

      // See if we can emit a new watermark, as we may have processed some pushed-back elements
      onWatermark(new Watermark(curInputWatermark));
      //LOG.info("{}, On watermark at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);
      System.out.println("Pushback latency: " + (System.currentTimeMillis() - st));
    } else {
      //LOG.info("Main element: {}", data);

      // This element is the Main Input
      checkAndInvokeBundle();
      final Iterable<WindowedValue<InputT>> pushedBack =
        getPushBackRunner().processElementInReadyWindows(data);
      int cnt = 0;
      for (final WindowedValue wv : pushedBack) {
        cnt += 1;
        curPushedBackWatermark = Math.min(curPushedBackWatermark, wv.getTimestamp().getMillis());
        curPushedBacks.add(wv);

        if (offloading) {
          if (byteBufList.isEmpty()) {
            final ByteBuf byteBuf = Unpooled.buffer();
            byteBufList.add(new ByteBufOutputStream(byteBuf));
          }

          final int lastIndex = byteBufList.size() - 1;
          if (byteBufList.get(lastIndex).buffer().readableBytes() > Constants.FLUSH_BYTES) {
            final ByteBuf byteBuf = Unpooled.buffer();
            byteBufList.add(new ByteBufOutputStream(byteBuf));
          }

          final ByteBufOutputStream bos = byteBufList.get(byteBufList.size() - 1);
          try {
            bos.writeBoolean(true);
            mainCoder.encode(wv, bos);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      }
      //System.out.println("pushback count: " + cnt);
      checkAndFinishBundle();
    }
  }


  private int handlePushBacksWithSideInput(final WindowedValue<SideInputElement> sideInput) {
    forceFinishBundle(); // forced


    checkAndInvokeBundle();
    long pushedBackAgainWatermark = Long.MAX_VALUE;
    final List<WindowedValue<InputT>> pushedBackAgain = new LinkedList<>();
    int cnt = 0;


    LOG.info("Start to offloading");
    final long st = System.currentTimeMillis();


    final PushBackOffloadingTransform offloadingTransform = new PushBackOffloadingTransform(
      doFn, inputCoder, outputCoders, mainOutputTag,
      additionalOutputTags, windowingStrategy, sideInputs, options, displayData);

    final ServerlessExecutorService<Pair<WindowedValue<SideInputElement>, List<WindowedValue<InputT>>>> slsExecutor =
      slsProvider.newCachedPool(offloadingTransform, offloadingSerializer, eventHandler);

    // encode side input
    for (final ByteBufOutputStream bos : byteBufList) {
      try {
        bos.writeBoolean(false);
        sideCoder.encode(sideInput, bos);
        bos.close();

        slsExecutor.execute(bos.buffer());
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }


    LOG.info("# of partition: {}, partitionSize: {}, execute latency: {}",
      byteBufList.size(), Constants.FLUSH_BYTES, System.currentTimeMillis() - st);

    final long st1 = System.currentTimeMillis();

    slsExecutor.shutdown();

    LOG.info("Time to wait shutdown {}, timestamp: {}", System.currentTimeMillis() - st1,
      System.currentTimeMillis());

    // TODO: fix
    //offloading = false;
    checkAndFinishBundle();

    // TODO: need to guarantee correctness
    byteBufList.clear();
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
    return new OutputCollector() {
      @Override
      public void emit(Object output) {
        LOG.info("Result {}", output);
        oc.emit(output);
      }

      @Override
      public void emitWatermark(Watermark watermark) {
        oc.emitWatermark(watermark);
      }

      @Override
      public void emit(String dstVertexId, Object output) {
        oc.emit(dstVertexId, output);
      }
    };
  }
}
