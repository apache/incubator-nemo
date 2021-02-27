package org.apache.nemo.runtime.executor.offloading;

import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingExecutorControlEvent;
import org.apache.nemo.offloading.common.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OffloadingDataTransportEventHandler
  implements EventHandler<Pair<Channel,OffloadingExecutorControlEvent>> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingDataTransportEventHandler.class.getName());
  private final Map<Channel, EventHandler<OffloadingExecutorControlEvent>> channelEventHandlerMap;


  @Inject
  private OffloadingDataTransportEventHandler() {
    //this.channelBufferMap = new ConcurrentHashMap<>();
    this.channelEventHandlerMap = new ConcurrentHashMap<>();
  }

  public Map<Channel, EventHandler<OffloadingExecutorControlEvent>> getChannelEventHandlerMap() {
    return channelEventHandlerMap;
  }

  @Override
  public void onNext(Pair<Channel, OffloadingExecutorControlEvent> nemoEvent) {
    final OffloadingExecutorControlEvent event = nemoEvent.right();
    LOG.info("OffloadingExecutorControlEvent received from channel {} {}", nemoEvent.left(), nemoEvent.right());
    while (!channelEventHandlerMap.containsKey(nemoEvent.left())) {
      LOG.info("No channel {} for {} // {}", nemoEvent.left(), event, channelEventHandlerMap.values());
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    channelEventHandlerMap.get(nemoEvent.left()).onNext(event);
  }
}
