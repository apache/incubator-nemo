package org.apache.nemo.offloading.common;


import io.netty.channel.Channel;

import java.io.Serializable;

public interface OffloadingTransform<I, O> extends Serializable {
  /**
   * Prepare the transform.
   * @param context of the transform.
   * @param outputCollector that collects outputs.
   */
  void prepare(OffloadingContext context, OffloadingOutputCollector<O> outputCollector);

  /**
   * On data received.
   * @param element data received.
   */
  void onData(I element, OffloadingOutputCollector oc);


  default boolean hasRemainingEvent() {
    return false;
  }

  /**
   * Close the transform.
   */
  void close();

  default void shutdownSchedule() {

  }

  default void schedule() {

  }

  String getDataChannelAddr();

  int getDataChannelPort();
  default Channel getDataChannel() {
    return null;
  }

  /**
   * Context of the transform.
   */
  interface OffloadingContext extends Serializable {
    Channel getControlChannel();
  }
}
