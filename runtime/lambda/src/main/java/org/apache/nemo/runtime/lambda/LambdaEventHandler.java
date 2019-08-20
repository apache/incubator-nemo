package org.apache.nemo.runtime.lambda;

import com.google.gson.Gson;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.LambdaEvent;

import java.util.concurrent.CountDownLatch;

public final class LambdaEventHandler {

  private Channel channel;
  private transient CountDownLatch workerComplete;

  public LambdaEventHandler(final CountDownLatch workerComplete) {
    this.workerComplete = workerComplete;
  }

  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  public synchronized void onNext(final LambdaEvent nemoEvent) {
    System.out.println("LambdaEventHandler->onNext " + nemoEvent.getType());
    switch (nemoEvent.getType()) {
      case WORKER_INIT:
        ByteBuf inBuffer = nemoEvent.getByteBuf();
        Task task = SerializationUtils.deserialize(new byte[inBuffer.readableBytes()]);

        try {
          System.out.println("Decode task successfully" + task.toString());
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println("Read LambdaEvent bytebuf error");
        }
        break;
      case DATA:
        throw new UnsupportedOperationException("DATA not supported");
      case END:
        // end of event
        System.out.println("END received");
        this.workerComplete.countDown();
        break;
      case WARMUP_END:
        throw new UnsupportedOperationException("WARMUP_END not supported");
    }
  }
}
