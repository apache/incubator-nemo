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

import com.google.common.io.CountingInputStream;
import com.google.common.io.CountingOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
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
import org.apache.nemo.compiler.frontend.beam.InMemorySideInputReader;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.compiler.frontend.beam.SideInputElement;
import org.apache.nemo.compiler.frontend.beam.coder.PushBackCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * DoFn transform implementation with push backs for side inputs.
 *
 * @param <InputT> input type.
 * @param <OutputT> output type.
 */
public final class PushBackDoFnTransform<InputT, OutputT> extends AbstractDoFnTransform<InputT, InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(PushBackDoFnTransform.class.getName());


  private final Coder mainCoder;
  private final Coder sideCoder;

  private StateStore stateStore;
  private String taskId;

  private transient List<WindowedValue<InputT>> curPushedBacks;
  private long curPushedBackWatermark; // Long.MAX_VALUE when no pushed-back exists.
  private long curInputWatermark;
  private long curOutputWatermark;

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
    this.curPushedBackWatermark = Long.MAX_VALUE;
    this.curInputWatermark = Long.MIN_VALUE;
    this.curOutputWatermark = Long.MIN_VALUE;
    this.mainCoder = mainCoder;
    this.sideCoder = sideCoder;
    LOG.info("Create pushback transform");
  }

  @Override
  public boolean isPushback() {
    return true;
  }

  @Override
  public void checkpoint(String checkpointId) {
    final StateStore stateStore = getContext().getStateStore();
    final long st = System.currentTimeMillis();
    try (final OutputStream os = stateStore.getOutputStream(checkpointId + "-pushback")) {
      LOG.info("Checkpointing pushback {}", taskId);
      final CountingOutputStream cos = new CountingOutputStream(os);
      final DataOutputStream dos = new DataOutputStream(cos);
      dos.writeLong(curPushedBackWatermark);
      dos.writeLong(curInputWatermark);
      dos.writeLong(curOutputWatermark);

      // push back data
      if (curPushedBacks != null) {
        dos.writeInt(curPushedBacks.size());
        for (final WindowedValue<InputT> elem : curPushedBacks) {
          mainCoder.encode(elem, dos);
        }
      } else {
        dos.writeInt(0);
      }

      // side input data
      getSideInputReader().checkpoint(dos, sideCoder);

      final long et = System.currentTimeMillis();
      LOG.info("Pushback encoding time of {}: {}, byte {}", taskId, et - st, cos.getCount());

    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Pushback checkpoint in " + checkpointId);
    }
  }

  @Override
  public void restore(final String id) {

    if (stateStore.containsState(getContext().getTaskId() + "-pushback")) {

      try (final InputStream is = stateStore.getStateStream(getContext().getTaskId() + "-pushback")) {
        LOG.info("Restoring pushback in restore {}", taskId);
        final long st = System.currentTimeMillis();

        final CountingInputStream countingInputStream = new CountingInputStream(is);
        final DataInputStream dis = new DataInputStream(countingInputStream);
        curPushedBackWatermark = dis.readLong();
        curInputWatermark = dis.readLong();
        curOutputWatermark = dis.readLong();

        final int size = dis.readInt();
        curPushedBacks = new LinkedList<>();
        for (int i = 0; i < size; i++) {
          curPushedBacks.add((WindowedValue<InputT>) mainCoder.decode(dis));
        }

        // decoding side input reader
        final InMemorySideInputReader newReader = InMemorySideInputReader.decode(getSideInputReader(), sideCoder, dis);
        getSideInputReader().restoreSideInput(newReader);

        final long et = System.currentTimeMillis();
        LOG.info("Decoding decoding pushback in restore {}: {} ms, byte {}", taskId, et - st, countingInputStream.getCount());
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  protected DoFn wrapDoFn(final DoFn initDoFn) {

    taskId = getContext().getTaskId();
    stateStore = getContext().getStateStore();

    if (stateStore.containsState(getContext().getTaskId() + "-pushback")) {
      final long st = System.currentTimeMillis();

      LOG.info("Restoring pushback in wrapDoFn {}", taskId);
      try (final InputStream is = stateStore.getStateStream(getContext().getTaskId() + "-pushback")) {
        final CountingInputStream countingInputStream = new CountingInputStream(is);
        final DataInputStream dis = new DataInputStream(countingInputStream);
        curPushedBackWatermark = dis.readLong();
        curInputWatermark = dis.readLong();
        curOutputWatermark = dis.readLong();

        final int size = dis.readInt();
         curPushedBacks = new LinkedList<>();
        for (int i = 0; i < size; i++) {
          curPushedBacks.add((WindowedValue<InputT>) mainCoder.decode(dis));
        }

        // decoding side input reader
        final InMemorySideInputReader newReader = InMemorySideInputReader.decode(getSideInputReader(), sideCoder, dis);
        getSideInputReader().restoreSideInput(newReader);


      final long et = System.currentTimeMillis();
      LOG.info("Decoding decoding pushback {}: {} ms, byte {}", taskId, et - st, countingInputStream.getCount());

      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    } else {
      curPushedBacks = new LinkedList<>();
    }


    return initDoFn;
  }

  public void setOffloading(boolean offload) {
  }

  @Override
  public void onData(final WindowedValue data) {

    // Need to distinguish side/main inputs and push-back main inputs.
    if (data.getValue() instanceof SideInputElement) {
      // This element is a Side Input
      final long st = System.currentTimeMillis();
      // LOG.info("Receive Side input at {}: {}", this.hashCode(), data);

      LOG.info("Side input start time: " + System.currentTimeMillis() + " " + data);

      // TODO #287: Consider Explicit Multi-Input IR Transform
      final WindowedValue<SideInputElement> sideInputElement = (WindowedValue<SideInputElement>) data;
      final PCollectionView view = getSideInputs().get(sideInputElement.getValue().getSideInputIndex());
      getSideInputReader().addSideInputElement(view, data);

      //LOG.info("{}, Add side input at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);

      int cnt;
      cnt = handlePushBacks();

      // System.out.println("Timestamp of pushback last " + System.currentTimeMillis());
      // LOG.info("{}, Handle pushback cnt: {} at {}: {}", System.currentTimeMillis() - st,
      //  cnt, this.hashCode(), data);

      // See if we can emit a new watermark, as we may have processed some pushed-back elements
      onWatermark(new Watermark(curInputWatermark));
      //LOG.info("{}, On watermark at {}: {}", System.currentTimeMillis() - st, this.hashCode(), data);
      LOG.info("Pushback latency: {}", (System.currentTimeMillis() - st));
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
      }
      //System.out.println("pushback count: " + cnt);
      checkAndFinishBundle();
    }
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
    return new OutputCollector(){
      long timestamp;
      @Override
      public void setInputTimestamp(long ts) {
        timestamp = ts;
      }

      @Override
      public long getInputTimestamp() {
        return timestamp;
      }

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
