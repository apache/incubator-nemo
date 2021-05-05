package org.apache.nemo.offloading.client;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.offloading.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nemo.offloading.common.OffloadingMasterEvent.Type.*;

public final class OffloadingEventHandler implements EventHandler<Pair<Channel,OffloadingMasterEvent>> {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingEventHandler.class.getName());
  private final BlockingQueue<Pair<Integer, Pair<Channel,OffloadingMasterEvent>>> handshakeQueue;
  private final BlockingQueue<Pair<Channel,OffloadingMasterEvent>> workerReadyQueue;
  private final BlockingQueue<Pair<Channel, OffloadingMasterEvent>> endQueue;
  private final AtomicInteger pendingRequest = new AtomicInteger();
  private final Map<Channel, EventHandler<OffloadingMasterEvent>> channelEventHandlerMap;
  private final Map<Integer, EventHandler<OffloadingMasterEvent>> requestIdHandlerMap;
  //private final Map<Channel, List<OffloadingMasterEvent>> channelBufferMap;

  private final ExecutorService executorService = Executors.newFixedThreadPool(5);

  private final Set<Integer> receivedRequests;

  public OffloadingEventHandler(final Map<Channel, EventHandler<OffloadingMasterEvent>> channelEventHandlerMap,
                                final Map<Integer, EventHandler<OffloadingMasterEvent>> requestIdHandlerMap) {
    this.handshakeQueue = new LinkedBlockingQueue<>();
    this.workerReadyQueue = new LinkedBlockingQueue<>();
    this.endQueue = new LinkedBlockingQueue<>();
    //this.channelBufferMap = new ConcurrentHashMap<>();
    this.channelEventHandlerMap = channelEventHandlerMap;
    this.receivedRequests = new HashSet<>();
    this.requestIdHandlerMap = requestIdHandlerMap;
  }

  public AtomicInteger getPendingRequest() {
    return pendingRequest;
  }

  public BlockingQueue<Pair<Channel, OffloadingMasterEvent>> getWorkerReadyQueue() {
    return workerReadyQueue;
  }

  public BlockingQueue<Pair<Integer, Pair<Channel, OffloadingMasterEvent>>> getHandshakeQueue() {
    return handshakeQueue;
  }

  @Override
  public void onNext(Pair<Channel, OffloadingMasterEvent> nemoEvent) {
    final OffloadingMasterEvent event = (OffloadingMasterEvent) nemoEvent.right();
    switch (event.getType()) {
      case CLIENT_HANDSHAKE:
        final int requestId = nemoEvent.right().getByteBuf().readInt();
        nemoEvent.right().getByteBuf().release();
        LOG.info("Client handshake from {}", nemoEvent.left().remoteAddress());
        synchronized (receivedRequests) {
          if (receivedRequests.contains(requestId)) {
            // duplicate id..
            LOG.info("Duplicate request id {}..., just finish this worker", requestId);
            requestIdHandlerMap.get(requestId).onNext(new OffloadingMasterEvent(DUPLICATE_REQUEST, nemoEvent.left(), 0));
            // nemoEvent.left().writeAndFlush(new OffloadingMasterEvent(DUPLICATE_REQUEST_TERMIATION, new byte[0], 0));
          } else {
            receivedRequests.add(requestId);
            handshakeQueue.add(Pair.of(requestId, nemoEvent));
          }
        }
        //nemoEvent.right().getByteBuf().release();
        break;
      case WORKER_INIT_DONE: {
        LOG.info("Worker is ready for channel {}", nemoEvent.left());
        workerReadyQueue.add(nemoEvent);
        break;
      }
      default:
        if (channelEventHandlerMap != null) {
          if (channelEventHandlerMap.containsKey(nemoEvent.left())) {

            /*
            if (channelBufferMap.containsKey(nemoEvent.left())) {
              LOG.info("Flushing buffered data for channel {}", nemoEvent.left());
              for (final OffloadingMasterEvent bufferedEvent : channelBufferMap.get(nemoEvent.left())) {
                channelEventHandlerMap.get(nemoEvent.left()).onNext(bufferedEvent);
              }
              channelBufferMap.remove(nemoEvent.left());
            }
            */

            executorService.execute(() -> {
              channelEventHandlerMap.get(nemoEvent.left()).onNext(event);
            });

          } else {
            if (event.getType().equals(ACTIVATE)) {
              throw new RuntimeException("No channel but received activate " + nemoEvent.left() + ", " + event);
            } else {
              LOG.info("No channel{} for {} // {}", nemoEvent.left(), event, channelEventHandlerMap.values());
            }
          }
        }
    }
  }
}
