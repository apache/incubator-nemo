package org.apache.nemo.runtime.executor.lambda;

import io.netty.channel.Channel;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.Pair;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class NemoEventHandler implements EventHandler<Pair<Channel,NemoEvent>> {

    private final BlockingQueue<Pair<Channel,NemoEvent>> handshakeQueue;
    private final BlockingQueue<Pair<Channel,NemoEvent>> resultQueue;
    private final BlockingQueue<Pair<Channel, NemoEvent>> endQueue;
    private final Map<Channel, EventHandler> channelEventHandlerMap;

    NemoEventHandler(final Map<Channel, EventHandler> channelEventHandlerMap) {
      this.handshakeQueue = new LinkedBlockingQueue<>();
      this.resultQueue = new LinkedBlockingQueue<>();
      this.endQueue = new LinkedBlockingQueue<>();
      this.channelEventHandlerMap = channelEventHandlerMap;
    }

    public BlockingQueue<Pair<Channel, NemoEvent>> getHandshakeQueue() {
      return handshakeQueue;
    }

    public BlockingQueue<Pair<Channel, NemoEvent>> getResultQueue() {
      return resultQueue;
    }

    public BlockingQueue<Pair<Channel, NemoEvent>> getEndQueue() {
      return endQueue;
    }


    @Override
    public void onNext(Pair<Channel,NemoEvent> nemoEvent) {
      final NemoEvent event = (NemoEvent) nemoEvent.right();
      switch (event.getType()) {
        case CLIENT_HANDSHAKE:
          handshakeQueue.add(nemoEvent);
          break;
        case RESULT:
          channelEventHandlerMap.get(nemoEvent.left()).onNext(nemoEvent.right());
          resultQueue.add(nemoEvent);
          break;
        case END:
          channelEventHandlerMap.get(nemoEvent.left()).onNext(nemoEvent.right());
          endQueue.add(nemoEvent);
          break;
        default:
          throw new IllegalStateException("Illegal type: " + event.getType().name());
      }
    }
  }
