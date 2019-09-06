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
package org.apache.nemo.runtime.executor.common.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.ChannelStatus;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.runtime.executor.common.ChannelStatus.RUNNING;

/**
 * Container for multiple input streams. Represents a transfer context on receiver-side.
 *
 * <h3>Thread safety:</h3>
 * <p>Methods with default access modifier, namely {@link #onNewStream()}, {@link #onByteBuf(ByteBuf)},
 * {@link #onContextClose()}, are not thread-safe, since they are called by a single Netty event loop.</p>
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public abstract class AbstractRemoteByteInputContext extends AbstractByteTransferContext implements ByteInputContext {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRemoteByteInputContext.class.getName());

  private final CompletableFuture<Iterator<InputStream>> completedFuture = new CompletableFuture<>();
  //private final Queue<ByteBufInputStream> byteBufInputStreams = new LinkedList<>();
  protected final ByteBufInputStream currentByteBufInputStream = new ByteBufInputStream();
  protected volatile boolean isFinished = false;

  protected EventHandler<Integer> ackHandler;
  protected final ScheduledExecutorService ackService;

  protected Channel currChannel;

  protected InputStreamIterator inputStreamIterator;
  protected TaskExecutor taskExecutor;
  protected DataFetcher dataFetcher;

  protected ChannelStatus channelStatus = RUNNING;

  protected volatile TaskLoc receiveDataFrom = VM;


  protected final TaskLoc myLocation;

  protected final ContextManager contextManager;

  protected String taskId;

  private Channel setupChannel;
  private TaskLoc setupLocation;

  /**
   * Creates an input context.
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public AbstractRemoteByteInputContext(final String remoteExecutorId,
                                        final ContextId contextId,
                                        final byte[] contextDescriptor,
                                        final ContextManager contextManager,
                                        final ScheduledExecutorService ackService,
                                        final TaskLoc location,
                                        final TaskLoc receiveDataFrom) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.ackService = ackService;
    this.currChannel = contextManager.getChannel();
    this.myLocation = location;
    this.contextManager = contextManager;
    this.receiveDataFrom = receiveDataFrom;
  }

  public <T> IteratorWithNumBytes<T> getInputIterator(
    final Serializer<?, T> serializer,
    final TaskExecutor te,
    final DataFetcher df) {
    inputStreamIterator = new InputStreamIterator<>(serializer);
    taskExecutor = te;
    dataFetcher = df;
    return inputStreamIterator;
  }

  public void setTaskId(String tid) {
    taskId = tid;
  }

  @Override
  public Iterator<InputStream> getInputStreams() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Iterator<InputStream>> getCompletedFuture() {
    throw new UnsupportedOperationException();
  }

  /**
   * Called when a punctuation for sub-stream incarnation is detected.
   */
  @Override
  public void onNewStream() {
    //currentByteBufInputStream = new ByteBufInputStream();
    //byteBufInputStreams.add(currentByteBufInputStream);
  }

  protected abstract ByteTransferContextSetupMessage getStopMessage();
  protected abstract void setupInputChannelToParentVM(TaskLoc sendDataTo);
  protected abstract void sendMessageToRelay(ByteTransferContextSetupMessage msg);

  @Override
  public synchronized void sendStopMessage(final EventHandler<Integer> handler) {
    ackHandler = handler;
    // send message to the upstream task!
    //LOG.info("Send message to remote: {}", message);

    switch (channelStatus) {
      case OUTPUT_STOP: {
        //LOG.info("Send stop after receiving output stop.. wait for restart {}/{}", taskId, getContextId().getTransferIndex());
        // output이 stop이면 그냥  ack.
        receivePendingAck();
        break;
      }
      case RUNNING: {
        //LOG.info("Send stop {}/{}", taskId, getContextId().getTransferIndex());
        channelStatus = ChannelStatus.INPUT_STOP;
        sendMessage(getStopMessage());
        break;
      }
    }
  }


  @Override
  public synchronized void receiveStopSignalFromParent(final ByteTransferContextSetupMessage msg, final TaskLoc sendDataTo) {

    switch (channelStatus) {
      case INPUT_STOP: {
        //LOG.info("Receive output stop after sending input stop {}/{} from {}", taskId, getContextId().getTransferIndex(),
        //  msg.getTaskId());
        // input stop인데 output을 받았다?
        receivePendingAck();
        setupInputChannelToParentVM(sendDataTo);
        break;
      }
      case RUNNING: {
        //LOG.info("Receive output stop {}/{} from {}", taskId, getContextId().getTransferIndex(), msg.getTaskId());
        channelStatus = ChannelStatus.OUTPUT_STOP;

        final ContextId contextId = getContextId();
        final byte[] contextDescriptor = getContextDescriptor();

        final ByteTransferContextSetupMessage ackMessage =
          new ByteTransferContextSetupMessage(contextId.getInitiatorExecutorId(),
            contextId.getTransferIndex(),
            contextId.getDataDirection(),
            contextDescriptor,
            contextId.isPipe(),
            ByteTransferContextSetupMessage.MessageType.ACK_FROM_CHILD_RECEIVE_PARENT_STOP_OUTPUT,
            myLocation,
            taskId);

        setupInputChannelToParentVM(sendDataTo);
        sendMessage(ackMessage);
        break;
      }
      default: {
        throw new RuntimeException("Unsupported status " + channelStatus);
      }
    }
  }

  /**
   * This is the key implementation.
   * We should send message differently according to the location
   */
  private void sendMessage(ByteTransferContextSetupMessage message) {
    if (myLocation.equals(SF) && receiveDataFrom.equals(SF)) {
      sendMessageToRelay(message);
    } else {
      currChannel.writeAndFlush(message);
    }
  }

  @Override
  public synchronized void receivePendingAck() {
    //LOG.info("Ack from parent stop output {}/{}", taskId, getContextId().getTransferIndex());
    // for guarantee
    taskExecutor.handleIntermediateData(inputStreamIterator, dataFetcher);

    taskExecutor.getExecutorThread().decoderThread.execute(() -> {
      taskExecutor.getExecutorThread().queue.add(() -> {
        ackHandler.onNext(1);
      });
    });
  }


  @Override
  public synchronized void receiveRestartSignalFromParent(Channel channel, ByteTransferContextSetupMessage msg) {
    currChannel = channel;
    receiveDataFrom = msg.getLocation();

    //LOG.info("Receive restart signal {}/{}", taskId, getContextId().getTransferIndex());
    channelStatus = RUNNING;
  }

  private volatile boolean restarted = false;

  protected String getLocalExecutorId() {
    if (getRemoteExecutorId().equals(getContextId().getInitiatorExecutorId())) {
      return getContextId().getPartnerExecutorId();
    } else {
      return getContextId().getInitiatorExecutorId();
    }
  }

  /**
   * restart 할때 restart signal을 보내야 하는데 parent task가 어디에 있는지에 대한 정보가 없음.
   * 그래서 필요함.
   * 이건 restart 전에 setup이 되어야함.
   */
  @Override
  public synchronized void setupRestartChannel(final Channel c, ByteTransferContextSetupMessage msg) {

    if (myLocation.equals(SF)) {
      throw new RuntimeException("This should not be called in serverless");
    }

    setupChannel = c;
    setupLocation = msg.getLocation();

    //LOG.info("Setup restart channel {} {}/{} for {}", msg.getLocation(), taskId, getContextId().getTransferIndex(),
    //  msg.getTaskId());

    if (restarted) {
      // TODO: send signal
        final ByteTransferContextSetupMessage restartMsg =
          new ByteTransferContextSetupMessage(getLocalExecutorId(),
            getContextId().getTransferIndex(),
            getContextId().getDataDirection(),
            getContextDescriptor(),
            getContextId().isPipe(),
            ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_CHILD_FOR_RESTART_OUTPUT,
            myLocation,
            taskId);

      // 기존 channel에 restart signal 날림.
      restarted = false;

      currChannel = setupChannel;
      receiveDataFrom = setupLocation;

      //LOG.info("Send restart message to parent 2: {}/ {} /{}", taskId, receiveDataFrom, currChannel);

      channelStatus = RUNNING;
      currChannel.writeAndFlush(restartMsg);
    } else {
      restarted = true;
    }
  }

  @Override
  public synchronized void restart(String taskId) {
    if (myLocation.equals(SF)) {
      throw new RuntimeException("Restart shouldn't be called in lambda");
    }

    if (restarted) {
      final ByteTransferContextSetupMessage restartMsg =
        new ByteTransferContextSetupMessage(getLocalExecutorId(),
          getContextId().getTransferIndex(),
          getContextId().getDataDirection(),
          getContextDescriptor(),
          getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_CHILD_FOR_RESTART_OUTPUT,
          VM,
          taskId);


      currChannel = setupChannel;
      receiveDataFrom = setupLocation;

      //LOG.info("Send restart message to parent 1: {}/ {} /{}", taskId, receiveDataFrom, currChannel);

      channelStatus = RUNNING;
      currChannel.writeAndFlush(restartMsg);

      restarted = false;
    } else {
      restarted = true;
    }
  }

  /**
   * Called when {@link ByteBuf} is supplied to this context.
   * @param byteBuf the {@link ByteBuf} to supply
   */
  @Override
  public void onByteBuf(final ByteBuf byteBuf) {
    if (byteBuf.readableBytes() > 0) {
      currentByteBufInputStream.byteBufQueue.put(byteBuf);
      // add it to the queue
      if (taskExecutor != null && inputStreamIterator != null && dataFetcher != null) {
        taskExecutor.handleIntermediateData(inputStreamIterator, dataFetcher);
      }
    } else {
      // ignore empty data frames
      byteBuf.release();
    }
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  /**
   * Called when {@link #onByteBuf(ByteBuf)} event is no longer expected.
   */
  @Override
  public void onContextClose() {
    isFinished = true;
    deregister();
  }

  @Override
  public void onContextStop() {
    isFinished = true;
  }

  @Override
  public void onContextRestart() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    cause.printStackTrace();
    throw new RuntimeException(cause);
    /*
    setChannelError(cause);
    if (currentByteBufInputStream != null) {
      currentByteBufInputStream.byteBufQueue.closeExceptionally(cause);
    }
    byteBufInputStreams.closeExceptionally(cause);
    completedFuture.completeExceptionally(cause);
    deregister();
    */
  }

  /**
   * An {@link InputStream} implementation that reads data from a composition of {@link ByteBuf}s.
   */
  private static final class ByteBufInputStream extends InputStream {

    private final ClosableBlockingQueue<ByteBuf> byteBufQueue = new ClosableBlockingQueue<>();

    @Override
    public int read() throws IOException {
      try {
        final ByteBuf head = byteBufQueue.peek();
        if (head == null) {
          // end of stream event
          return -1;
        }
        final int b = head.readUnsignedByte();
        if (head.readableBytes() == 0) {
          // remove and release header if no longer required
          byteBufQueue.take();
          head.release();
        }
        return b;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public int read(final byte[] bytes, final int baseOffset, final int maxLength) throws IOException {
      if (bytes == null) {
        throw new NullPointerException();
      }
      if (baseOffset < 0 || maxLength < 0 || maxLength > bytes.length - baseOffset) {
        throw new IndexOutOfBoundsException();
      }
      try {
        // the number of bytes that has been read so far
        int readBytes = 0;
        // the number of bytes to read
        int capacity = maxLength;
        while (capacity > 0) {
          final ByteBuf head = byteBufQueue.peek();
          if (head == null) {
            // end of stream event
            return readBytes == 0 ? -1 : readBytes;
          }
          final int toRead = Math.min(head.readableBytes(), capacity);
          head.readBytes(bytes, baseOffset + readBytes, toRead);
          if (head.readableBytes() == 0) {
            byteBufQueue.take();
            head.release();
          }
          readBytes += toRead;
          capacity -= toRead;
        }
        return readBytes;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public long skip(final long n) throws IOException {
      if (n <= 0) {
        return 0;
      }
      try {
        // the number of bytes that has been skipped so far
        long skippedBytes = 0;
        // the number of bytes to skip
        long toSkip = n;
        while (toSkip > 0) {
          final ByteBuf head = byteBufQueue.peek();
          if (head == null) {
            // end of stream event
            return skippedBytes;
          }
          if (head.readableBytes() > toSkip) {
            head.skipBytes((int) toSkip);
            skippedBytes += toSkip;
            return skippedBytes;
          } else {
            // discard the whole ByteBuf
            skippedBytes += head.readableBytes();
            toSkip -= head.readableBytes();
            byteBufQueue.take();
            head.release();
          }
        }
        return skippedBytes;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public int available() throws IOException {
      try {
        final ByteBuf head = byteBufQueue.peek();
        if (head == null) {
          return 0;
        } else {
          return head.readableBytes();
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
  }

  public final class InputStreamIterator<T> implements IteratorWithNumBytes<T> {

    private final Serializer<?, T> serializer;
    private volatile T next;
    private final DecoderFactory.Decoder<T> decoder;

    public InputStreamIterator(final Serializer<?, T> serializer) {
      this.serializer = serializer;
      try {
        this.decoder = serializer.getDecoderFactory().create(currentByteBufInputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isFinished() {
      return isFinished;
    }

    @Override
    public long getNumSerializedBytes() throws NumBytesNotSupportedException {
      return 0;
    }

    @Override
    public long getNumEncodedBytes() throws NumBytesNotSupportedException {
      return 0;
    }

    @Override
    public boolean hasNext() {
      if (currentByteBufInputStream.byteBufQueue.isEmpty() || isFinished) {
        return false;
      }
      return true;
    }

    @Override
    public T next() {
      try {
        return decoder.decode();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  final class AckRunner implements Runnable {

    @Override
    public void run() {
      //LOG.info("Bytebuf: {}", currentByteBufInputStream.byteBufQueue.isEmpty());
      if (currentByteBufInputStream.byteBufQueue.isEmpty()) {
        ackHandler.onNext(1);
      } else {
        ackService.schedule(new AckRunner(), 500, TimeUnit.MILLISECONDS);
      }
    }
  }
}
