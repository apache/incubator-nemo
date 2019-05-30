package org.apache.nemo.offloading.client;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingEvent;
import org.apache.nemo.offloading.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class OffloadingEventHandler implements EventHandler<Pair<Channel,OffloadingEvent>> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingEventHandler.class.getName());
  private final BlockingQueue<Pair<Channel,OffloadingEvent>> handshakeQueue;
  private final BlockingQueue<Pair<Channel, OffloadingEvent>> readyQueue;
  private final BlockingQueue<Pair<Channel, OffloadingEvent>> endQueue;
  private final AtomicInteger pendingRequest = new AtomicInteger();
  private final Map<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap;
  private final Map<Channel, List<OffloadingEvent>> channelBufferMap;

  public OffloadingEventHandler(final Map<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap) {
    this.handshakeQueue = new LinkedBlockingQueue<>();
    this.readyQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();
    this.channelBufferMap = new ConcurrentHashMap<>();
    this.channelEventHandlerMap = channelEventHandlerMap;
  }

  public OffloadingEventHandler() {
    this.handshakeQueue = new LinkedBlockingQueue<>();
    this.readyQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();
    this.channelEventHandlerMap = null;
  }

  public AtomicInteger getPendingRequest() {
    return pendingRequest;
  }

  public BlockingQueue<Pair<Channel, OffloadingEvent>> getReadyQueue() {
    return readyQueue;
  }

  public BlockingQueue<Pair<Channel, OffloadingEvent>> getHandshakeQueue() {
    return handshakeQueue;
  }

  @Override
  public void onNext(Pair<Channel,OffloadingEvent> nemoEvent) {
    final OffloadingEvent event = (OffloadingEvent) nemoEvent.right();
    switch (event.getType()) {
      case CLIENT_HANDSHAKE:
        handshakeQueue.add(nemoEvent);
        //nemoEvent.right().getByteBuf().release();
        break;
      default:
        if (channelEventHandlerMap != null) {
          if (channelEventHandlerMap.containsKey(nemoEvent.left())) {

            if (channelBufferMap.containsKey(nemoEvent.left())) {
              LOG.info("Flushing buffered data for channel {}", nemoEvent.left());
              for (final OffloadingEvent bufferedEvent : channelBufferMap.get(nemoEvent.left())) {
                channelEventHandlerMap.get(nemoEvent.left()).onNext(bufferedEvent);
              }
              channelBufferMap.remove(nemoEvent.left());
            }
            channelEventHandlerMap.get(nemoEvent.left()).onNext(event);
          } else {
            LOG.info("No channel {} for {} // {}", nemoEvent.left(), event, channelEventHandlerMap.values());
            channelBufferMap.putIfAbsent(nemoEvent.left(), new LinkedList<>());
            channelBufferMap.get(nemoEvent.left()).add(event);
          }
        }
    }
  }
}
