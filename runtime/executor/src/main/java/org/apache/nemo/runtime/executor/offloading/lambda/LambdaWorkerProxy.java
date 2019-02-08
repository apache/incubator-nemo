package org.apache.nemo.runtime.executor.offloading.lambda;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.OffloadingWorkerFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.OffloadingWorker;
import org.apache.nemo.common.lambda.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public final class LambdaWorkerProxy implements OffloadingWorker {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaWorkerProxy.class.getName());
  private volatile Channel channel;
  private final BlockingQueue resultQueue;
  private final BlockingQueue<NemoEvent> endQueue;
  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap;

  private final EncoderFactory inputEncoderFactory;
  private final DecoderFactory inputDecoderFactory;

  private final EncoderFactory outputEncoderFactory;
  private final DecoderFactory outputDecoderFactory;


  private EncoderFactory.Encoder encoder;

  private final OffloadingWorkerFactory offloadingWorkerFactory;

  private final Future<Channel> channelFuture;

  // two data stream: when channel is null, we buffer byte in byte array output
  // otherwise, we use byteBufOutputStream directly.
  private ObjectOutputStream objectOutputStream;
  private ByteBufOutputStream byteBufOutputStream;



  public LambdaWorkerProxy(final Future<Channel> channelFuture,
                           final OffloadingWorkerFactory offloadingWorkerFactory,
                           final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap,
                           final EncoderFactory inputEncoderFactory,
                           final DecoderFactory inputDecoderFactory,
                           final EncoderFactory outputEncoderFactory,
                           final DecoderFactory outputDecoderFactory) {
    this.channelFuture = channelFuture;
    this.offloadingWorkerFactory = offloadingWorkerFactory;

    this.channelEventHandlerMap = channelEventHandlerMap;
    this.resultQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();

    this.inputEncoderFactory = inputEncoderFactory;
    this.inputDecoderFactory = inputDecoderFactory;
    this.outputEncoderFactory = outputEncoderFactory;
    this.outputDecoderFactory = outputDecoderFactory;

    final ByteBuf byteBuffer = Unpooled.directBuffer(Constants.FLUSH_BYTES);
    this.byteBufOutputStream = new ByteBufOutputStream(byteBuffer);
    try {
      byteBufOutputStream.writeInt(NemoEvent.Type.DATA.ordinal());
      this.encoder = inputEncoderFactory.create(byteBufOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    final ExecutorService channelThread = Executors.newSingleThreadExecutor();
    channelThread.execute(() -> {
      try {
        channel = channelFuture.get();
        channelEventHandlerMap.put(channel, new EventHandler<NemoEvent>() {
          @Override
          public void onNext(NemoEvent msg) {
            switch (msg.getType()) {
              case RESULT:
                LOG.info("Receive result");
                final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
                try {
                  final DecoderFactory.Decoder decoder = outputDecoderFactory.create(bis);
                  final Object data = decoder.decode();
                  resultQueue.add(data);
                  msg.getByteBuf().release();
                } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException();
                }
                break;
              case END:
                LOG.info("Receive end");
                msg.getByteBuf().release();
                endQueue.add(msg);
                break;
              default:
                throw new RuntimeException("Invalid type: " + msg);
            }
          }
        });
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public <T> List<T> getResult() {
    //LOG.info("Get result");

    if (channel == null) {
      throw new RuntimeException("Channel should not be null");
    }


    final List<T> result = new ArrayList<>();

    while (endQueue.peek() == null) {
      while (!resultQueue.isEmpty()) {
        result.add((T) resultQueue.poll());
      }

      //LOG.info("Result is empty, but don't receive end message");

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    //LOG.info("We received end message!");

    while (!resultQueue.isEmpty()) {
      result.add((T) resultQueue.poll());
    }

    return result;
  }

  @Override
  public void write(final Object input) {
    try {
      encoder.encode(input);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    if (isFlushTime()) {
      flush();
    }
  }

  private boolean isFlushTime() {
    return byteBufOutputStream.buffer().readableBytes() >= Constants.FLUSH_BYTES;
  }

  @Override
  public void flush() {
    try {
      if (channel != null) {
        byteBufOutputStream.close();
        channel.writeAndFlush(new NemoEvent(NemoEvent.Type.DATA, byteBufOutputStream.buffer()));
        byteBufOutputStream = new ByteBufOutputStream(channel.alloc().ioBuffer(Constants.FLUSH_BYTES + 1000));
        byteBufOutputStream.writeInt(NemoEvent.Type.DATA.ordinal());
        encoder = inputEncoderFactory.create(byteBufOutputStream);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishOffloading() {
    try {
      byteBufOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (channel != null) {
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0));
      byteBufOutputStream.buffer().release();
    } else {
      // waiting for channel
      while (channel == null) {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // send buffered data
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.DATA, byteBufOutputStream.buffer()));
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0));
    }

    offloadingWorkerFactory.deleteOffloadingWorker(this);
  }
}
