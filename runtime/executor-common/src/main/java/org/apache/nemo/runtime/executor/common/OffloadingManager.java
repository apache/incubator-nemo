package org.apache.nemo.runtime.executor.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.Pair;

import java.util.Optional;
import java.util.concurrent.Future;

public interface OffloadingManager {
  void offloading(String taskId, byte[] serializedDag);
  void writeData(String taskId, TaskHandlingEvent data);
}
