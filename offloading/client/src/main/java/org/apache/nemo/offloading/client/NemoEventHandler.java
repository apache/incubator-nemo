package org.apache.nemo.offloading.client;

import io.netty.channel.Channel;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class NemoEventHandler implements EventHandler<Pair<Channel,NemoEvent>> {
  private static final Logger LOG = LoggerFactory.getLogger(NemoEventHandler.class.getName());
  private final BlockingQueue<Pair<Channel,NemoEvent>> handshakeQueue;
  private final BlockingQueue<Pair<Channel, NemoEvent>> readyQueue;
  private final BlockingQueue<Pair<Channel, NemoEvent>> endQueue;
  private final AtomicInteger pendingRequest = new AtomicInteger();
  private final Map<Channel, EventHandler<NemoEvent>> channelEventHandlerMap;

  public NemoEventHandler(final Map<Channel, EventHandler<NemoEvent>> channelEventHandlerMap) {
    this.handshakeQueue = new LinkedBlockingQueue<>();
    this.readyQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();
    this.channelEventHandlerMap = channelEventHandlerMap;
  }

  public NemoEventHandler() {
    this.handshakeQueue = new LinkedBlockingQueue<>();
    this.readyQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();
    this.channelEventHandlerMap = null;
  }

  public AtomicInteger getPendingRequest() {
    return pendingRequest;
  }

  public BlockingQueue<Pair<Channel, NemoEvent>> getReadyQueue() {
    return readyQueue;
  }

  public BlockingQueue<Pair<Channel, NemoEvent>> getHandshakeQueue() {
    return handshakeQueue;
  }

  @Override
  public void onNext(Pair<Channel,NemoEvent> nemoEvent) {
    final NemoEvent event = (NemoEvent) nemoEvent.right();
    switch (event.getType()) {
      case CLIENT_HANDSHAKE:
        handshakeQueue.add(nemoEvent);
        //nemoEvent.right().getByteBuf().release();
        break;
      default:
        if (channelEventHandlerMap != null) {
          if (channelEventHandlerMap.containsKey(nemoEvent.left())) {
            channelEventHandlerMap.get(nemoEvent.left()).onNext(event);
          } else {
            LOG.info("No channel {} for {} // {}", nemoEvent.left(), event, channelEventHandlerMap.values());
          }
        }
    }
  }
}
