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
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.runtime.executor.common.ChannelStatus;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayControlFrame;
import org.apache.nemo.runtime.executor.common.relayserverclient.RelayDataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.nemo.common.TaskLoc.SF;
import static org.apache.nemo.common.TaskLoc.VM;
import static org.apache.nemo.runtime.executor.common.ChannelStatus.RUNNING;

/**
 * Container for multiple output streams. Represents a transfer context on sender-side.
 *
 * <p>Public methods are thread safe,
 * although the execution order may not be linearized if they were called from different threads.</p>
 */
public abstract class AbstractRemoteByteOutputContext extends AbstractByteTransferContext implements ByteOutputContext {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRemoteByteOutputContext.class.getName());

  private Channel channel;
  private String vmTaskId;

  private volatile boolean closed = false;

  enum Status {
    PENDING_INIT,
    PENDING,
    NO_PENDING
  }

  private volatile Status currStatus = Status.NO_PENDING;

  private TaskLoc sendDataTo;

  private EventHandler<Integer> ackHandler;

  private volatile boolean settingContext = false;

  //protected String taskId;

  private final Object writeLock = new Object();

  protected ExecutorThread executorThread;

  private final TaskLoc myLocation;


  private ChannelStatus channelStatus;

  private final String relayDst;

  protected final ContextManager contextManager;

  protected String taskId = null;

  private Channel setupChannel;
  private TaskLoc setupLocation;

  private final List<Runnable> pendingRunnables = new ArrayList<>();

  /**
   * Creates a output context.
   *
   * @param remoteExecutorId    id of the remote executor
   * @param contextId           identifier for this context
   * @param contextDescriptor   user-provided context descriptor
   * @param contextManager      {@link ContextManager} for the channel
   */
  public AbstractRemoteByteOutputContext(final String remoteExecutorId,
                                         final ContextId contextId,
                                         final byte[] contextDescriptor,
                                         final ContextManager contextManager,
                                         final TaskLoc myLocation,
                                         final TaskLoc sdt,
                                         final String relayDst) {
    super(remoteExecutorId, contextId, contextDescriptor, contextManager);
    this.channelStatus = ChannelStatus.RUNNING;
    this.myLocation = myLocation;
    this.sendDataTo = sdt;
    this.relayDst = relayDst;
    this.contextManager = contextManager;
    this.channel = contextManager.getChannel();
  }

  protected String getLocalExecutorId() {
    if (getRemoteExecutorId().equals(getContextId().getInitiatorExecutorId())) {
      return getContextId().getPartnerExecutorId();
    } else {
      return getContextId().getInitiatorExecutorId();
    }
  }

  private ByteTransferContextSetupMessage getStopMsg() {
    final ByteTransferContextSetupMessage pendingMsg =
      new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
        getContextId().getTransferIndex(),
        getContextId().getDataDirection(),
        getContextDescriptor(),
        getContextId().isPipe(),
        ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_STOPPING_OUTPUT,
        myLocation.equals(VM) ? SF : VM,
        taskId);
    return pendingMsg;
  }

  private void sendControlFrame(ByteTransferContextSetupMessage message) {
    if (myLocation.equals(SF) && sendDataTo.equals(SF)) {
      //LOG.info("Send message to relay server {} / {} / {}", relayDst, taskId, message);
      channel.writeAndFlush(new RelayControlFrame(relayDst, message));
    } else {
      //LOG.info("Send message to VM {} / {}", message, taskId);
      channel.writeAndFlush(message);
    }
  }

  @Override
  public void setTaskId(final String tid) {
    taskId = tid;
  }

  @Override
  public synchronized void sendStopMessage(final EventHandler<Integer> handler) {
    //LOG.info("Send message from {}/{} to remote: {}", getContextId().getTransferIndex(), sendDataTo, message);
    ackHandler = handler;

    final ByteTransferContextSetupMessage message = getStopMsg();

    switch (channelStatus) {
      case INPUT_STOP: {
        //  input이 이미 stop이면 걍 올림.
        //LOG.info("Wait for input restart {}/{}", taskId, getContextId().getTransferIndex());
        //channelStatus = WAIT_FOR_INPUT_RESTART;
        //LOG.info("Output stop input stop ack {}/{}", taskId, getContextId().getTransferIndex());
        ackHandler.onNext(1);
        break;
      }
      case RUNNING: {
        //LOG.info("Output stop {}/{}", taskId, getContextId().getTransferIndex());
        channelStatus = ChannelStatus.OUTPUT_STOP;

        executorThread.queue.add(() -> {
          try {
            currStatus = Status.PENDING;
            sendControlFrame(message);
          } catch (final Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        });

        //LOG.info("Executor queue size {} {}/{}", executorThread.hashCode(), taskId, executorThread.queue.size());
        break;
      }
      default:
        throw new RuntimeException("Unsupported channel status " + channelStatus);
    }
  }

  @Override
  public void receiveStopAck() {
    //LOG.info("Ack output stop {}/{}", taskId, getContextId().getTransferIndex());
    ackHandler.onNext(1);
  }

  /**
   * Closes existing sub-stream (if any) and create a new sub-stream.
   * @return new {@link ByteOutputStream}
   * @throws IOException if an exception was set or this context was closed.
   */
  @Override
  public synchronized ByteOutputStream newOutputStream(final ExecutorThread t) throws IOException {
    ensureNoException();
    if (closed) {
      throw new IOException("Context already closed.");
    }

    executorThread = t;

    if (!pendingRunnables.isEmpty()) {
      executorThread.queue.addAll(pendingRunnables);
      pendingRunnables.clear();
    }

    return new RemoteByteOutputStream();
  }

  protected abstract void setupOutputChannelToParentVM(ByteTransferContextSetupMessage msg, TaskLoc sdt);

  @Override
  public synchronized void receiveStopSignalFromChild(final ByteTransferContextSetupMessage msg, final TaskLoc sdt) {
    //TODO: location 바꿔줘야함.

    synchronized (writeLock) {
      switch (channelStatus) {
        case OUTPUT_STOP: {
          // it means that we already send the ack (OS)
          // 왜냐면 이미 stopping한다고 signal이 갔기 때문에 input에서 이를 ack으로 취급함.

          //LOG.info("Receive input stop from {} after sending output stop {}/{}",
          //  msg.getTaskId(), taskId, getContextId().getTransferIndex());

          executorThread.queue.add(() -> {
            setupOutputChannelToParentVM(msg, sdt);
          });
          break;
        }
        case RUNNING: {
          //LOG.info("Receive input stop from {}.. {}/{}", msg.getTaskId(), taskId, getContextId().getTransferIndex());
          channelStatus = ChannelStatus.INPUT_STOP;
          executorThread.queue.add(() -> {
            currStatus = Status.PENDING;
            final ByteTransferContextSetupMessage message =
              new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
                getContextId().getTransferIndex(),
                getContextId().getDataDirection(),
                getContextDescriptor(),
                getContextId().isPipe(),
                ByteTransferContextSetupMessage.MessageType.ACK_FROM_PARENT_STOP_OUTPUT,
                myLocation,
                taskId);
            //LOG.info("Ack receiveStopSignalFromChild to {}, change to {}, {}",sendDataTo,  sdt, message);

            setupOutputChannelToParentVM(msg, sdt);
            sendControlFrame(message);
          });
          break;
        }
        default:
          throw new RuntimeException("Invalid channel status");
      }
    }
  }

  public Channel getChannel() {
    return channel;
  }


  @Override
  public synchronized void receiveRestartSignalFromChild(Channel c,
                                                         ByteTransferContextSetupMessage msg) {
    channel = c;
    sendDataTo = msg.getLocation();

    //LOG.info("Receive input restart from {}/{}.. {}/{}", msg.getTaskId(), sendDataTo,
    //  taskId, getContextId().getTransferIndex());
        /*
        final ByteTransferContextSetupMessage message =
          new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
            getContextId().getTransferIndex(),
            getContextId().getDataDirection(), getContextDescriptor(),
            getContextId().isPipe(),
            ByteTransferContextSetupMessage.MessageType.ACK_FROM_PARENT_RESTART_OUTPUT,
            myLocation,
            taskId);
        sendControlFrame(message);
        */

    channelStatus = ChannelStatus.RUNNING;

    if (executorThread == null) {
      pendingRunnables.add(() -> {
        currStatus = Status.NO_PENDING;
      });
    } else {
      executorThread.queue.add(() -> {
        currStatus = Status.NO_PENDING;
      });
    }
  }

  private volatile boolean restarted = false;

  @Override
  public synchronized void setupRestartChannel(final Channel c, final ByteTransferContextSetupMessage msg) {

    if (myLocation.equals(SF)) {
      throw new RuntimeException("This should not be called in serverless");
    }

    setupChannel = c;
    setupLocation = msg.getLocation();

    if (restarted) {
      // TODO: send signal
      final ByteTransferContextSetupMessage message =
        new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
          getContextId().getTransferIndex(),
          getContextId().getDataDirection(), getContextDescriptor(),
          getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,
          myLocation,
          taskId);

      //LOG.info("Setting up serverless channel");
      // 기존 channel에 restart signal 날림.
      restarted = false;

      sendDataTo = setupLocation;
      channel = setupChannel;

      channelStatus = RUNNING;
      channel.writeAndFlush(message).addListener(getChannelWriteListener());
      executorThread.queue.add(() -> {
        currStatus = Status.NO_PENDING;
      });
    } else {
      restarted = true;
    }
  }

  @Override
  public synchronized void receiveRestartAck() {
    // TODO: restart에서 ack을 받아야 하는지?
  }

  /**
   * Restart 할때까 특별한 case임.
   * VM -> SF 로 connection을 initiate 할 수 없기 때문에 SF -> VM으로 connection이 initiate 되어야 하는데
   * 언제? SF에서 output stop signal을 받을 때
   */
  @Override
  public synchronized void restart(final String taskId) {
    if (myLocation.equals(SF)) {
      throw new RuntimeException("Restart shouldn't be called in lambda");
    }

    if (restarted) {
      final ByteTransferContextSetupMessage message =
        new ByteTransferContextSetupMessage(getContextId().getInitiatorExecutorId(),
          getContextId().getTransferIndex(),
          getContextId().getDataDirection(), getContextDescriptor(),
          getContextId().isPipe(),
          ByteTransferContextSetupMessage.MessageType.SIGNAL_FROM_PARENT_RESTARTING_OUTPUT,
          VM,
          taskId);

      sendDataTo = setupLocation;
      channel = setupChannel;

      // channel에 restart signal 날림.
      channelStatus = RUNNING;
      channel.writeAndFlush(message).addListener(getChannelWriteListener());
      executorThread.queue.add(() -> {
        currStatus = Status.NO_PENDING;
      });

      //LOG.info("Restart {} output", taskId);

      restarted = false;
    } else {
      restarted = true;
    }
  }

  /**
   * Closes this stream.
   *
   * @throws IOException if an exception was set
   */
  @Override
  public void close() throws IOException {
    ensureNoException();
    if (closed) {
      return;
    }
    channel.writeAndFlush(DataFrameEncoder.DataFrame.newInstance(getContextId()))
      .addListener(getChannelWriteListener());
    deregister();
    closed = true;
  }

  @Override
  public void onChannelError(@Nullable final Throwable cause) {
    cause.printStackTrace();
    throw new RuntimeException("Channel closed erorr " + channel + " execption " + cause.getMessage());
    //setChannelError(cause);
    //channel.close();
  }

  /**
   * @throws IOException when a channel exception has been set.
   */
  void ensureNoException() throws IOException {
    if (hasException()) {
      if (getException() == null) {
        throw new IOException();
      } else {
        throw new IOException(getException());
      }
    }
  }

  /**
   * An {@link OutputStream} implementation which buffers data to {@link ByteBuf}s.
   *
   * <p>Public methods are thread safe,
   * although the execution order may not be linearized if they were called from different threads.</p>
   */
  public final class RemoteByteOutputStream extends OutputStream implements ByteOutputStream {

    private volatile boolean newSubStream = true;
    private volatile boolean closed = false;
    private final List<ByteBuf> pendingByteBufs = new ArrayList<>();

    public Channel getChannel() {
      return channel;
    }

    @Override
    public void write(final int i) throws IOException {
      final ByteBuf byteBuf = channel.alloc().ioBuffer(1, 1);
      byteBuf.writeByte(i);
      writeByteBuf(byteBuf);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      final ByteBuf byteBuf = channel.alloc().ioBuffer(length, length);
      byteBuf.writeBytes(bytes, offset, length);
      writeByteBuf(byteBuf);
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      if (newSubStream) {
        // to emit a frame with new sub-stream flag
        writeDataFrame(null, 0, true);
      }

      channel.flush();
      closed = true;
    }

    /**
     * Writes a data frame, from {@link ByteBuf}.
     * @param byteBuf {@link ByteBuf} to write.
     */
    private void writeByteBuf(final ByteBuf byteBuf) throws IOException {
      if (byteBuf.readableBytes() > 0) {
        writeDataFrame(byteBuf, byteBuf.readableBytes(), true);
      }
    }

    /**
     * Write an element to the channel.
     * @param element element
     * @param serializer serializer
     */
    @Override
    public void writeElement(final Object element,
                             final Serializer serializer,
                             final String edgeId,
                             final String opId) {

      //LOG.info("Writing element in {} to {}/{}, receiveStopSignalFromChild: {}, pendingBytes: {}",
      //  getContextId().getTransferIndex(), edgeId, opId, currStatus, pendingByteBufs.size());

      final ByteBuf byteBuf = channel.alloc().ioBuffer();
      final ByteBufOutputStream byteBufOutputStream = new ByteBufOutputStream(byteBuf);

      try {
        final OutputStream wrapped = byteBufOutputStream;
          //DataUtil.buildOutputStream(byteBufOutputStream, serializer.getEncodeStreamChainers());
        final EncoderFactory.Encoder encoder = serializer.getEncoderFactory().create(wrapped);
        //LOG.info("Element encoder: {}", encoder);
        encoder.encode(element);
        wrapped.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      //LOG.info("Write element {} / {} / {} / {} ", taskId, currStatus, sendDataTo, channel);

      synchronized (writeLock) {
        try {
          switch (currStatus) {
            case PENDING: {
              pendingByteBufs.add(byteBuf);
              break;
            }
            case NO_PENDING: {
              if (!pendingByteBufs.isEmpty()) {
                //LOG.info("[Send receiveStopSignalFromChild events: {}]", pendingByteBufs.size());
                for (final ByteBuf pendingByteBuf : pendingByteBufs) {
                  writeByteBuf(pendingByteBuf);
                }
                pendingByteBufs.clear();
              }
              writeByteBuf(byteBuf);
              break;
            }
            default: {
              throw new RuntimeException("Unsupported status " + currStatus);
            }
          }
        } catch (final IOException e) {
          throw new RuntimeException("curr status: " + currStatus + ", to " + sendDataTo + " restart? " + restarted + " channel " + channel + " " + e);
        }
      }
    }

    @Override
    public void flush() {
      synchronized (writeLock) {
        channel.flush();
      }
    }

    /**
     * Writes a data frame.
     * @param body        the body or {@code null}
     * @param length      the length of the body, in bytes
     * @throws IOException when an exception has been set or this stream was closed
     */
    private void writeDataFrame(
      final Object body, final long length, final boolean openSubStream) throws IOException {
      ensureNoException();
      if (closed) {
        throw new IOException("Stream already closed.");
      }

      switch (myLocation) {
        case SF: {
          switch (sendDataTo) {
            case SF: {
              channel.write(new RelayDataFrame(relayDst,
                DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream)))
                .addListener(getChannelWriteListener());
              break;
            }
            case VM: {
              channel.write(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream))
                .addListener(getChannelWriteListener());
              break;
            }
          }
          break;
        }
        case VM: {
          channel.write(DataFrameEncoder.DataFrame.newInstance(getContextId(), body, length, openSubStream))
            .addListener(getChannelWriteListener());
          break;
        }
      }

    }
  }
}
