package edu.snu.vortex.runtime.executor;

import edu.snu.vortex.runtime.*;
import edu.snu.vortex.runtime.driver.Parameters;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class BlockManager {

  private static final Logger LOG = Logger.getLogger(BlockManager.class.getName());

  private static BlockManager instance; // Hack. Don't use this approach in master code.
  private final VortexExecutor vortexExecutor;
  private final NetworkConnectionService ncs;
  private final IdentifierFactory idFactory;
  private final ConcurrentMap<String, List> channelIdToDataMap;
  private final NetworkEventHandler networkEventHandler;
  private final AtomicReference<ConnectionFactory<DataMessage>> connectionFactory;

  private final ConcurrentMap<String, CountDownLatch> latchMap;

  BlockManager(
      final VortexExecutor vortexExecutor,
      final NetworkConnectionService ncs,
      final IdentifierFactory idFactory) {
    this.vortexExecutor = vortexExecutor;
    this.ncs = ncs;
    this.idFactory = idFactory;
    this.channelIdToDataMap = new ConcurrentHashMap<>();
    this.networkEventHandler = new NetworkEventHandler();
    this.connectionFactory = new AtomicReference<>();
    this.latchMap = new ConcurrentHashMap<>();
    instance = this;
  }

  private ConnectionFactory<DataMessage> getConnectionFactory() {
    ConnectionFactory<DataMessage> cf = connectionFactory.get();
    if (cf == null) {
      cf = ncs.getConnectionFactory(idFactory.getNewInstance(Parameters.NCS_ID));
      if (connectionFactory.compareAndSet(null, cf)) // CAS succeeded
        return cf;
      else
        return connectionFactory.get();
    } else {
      return cf;
    }
  }

  private final class NetworkEventHandler implements EventHandler<Message<DataMessage>> {

    @Override
    public void onNext(final Message<DataMessage> message) {
      final DataMessage dataMessage = message.getData().iterator().next();
      System.out.println("Received data for channel " + dataMessage.getChannelId() + " , " + dataMessage.getData());

      channelIdToDataMap.put(dataMessage.getChannelId(), dataMessage.getData());
      latchMap.remove(dataMessage.getChannelId()).countDown();
    }
  }

  void onNotReadyResponse(final String channelId) {
    System.out.println("Channel " + channelId + " is not ready");
    latchMap.remove(channelId).countDown();
  }

  EventHandler<Message<DataMessage>> getNetworkEventHandler() {
    return networkEventHandler;
  }

  public synchronized void write(Channel channel, List data) {
    // System.out.println(channel + " Write: " + data);
    final List existingData = channelIdToDataMap.get(channel.getId());
    if (existingData != null) {
      existingData.addAll(data);
    } else {
      channelIdToDataMap.put(channel.getId(), data);
    }
  }

  public synchronized List read(Channel channel) {
    final String channelId = channel.getId();
    final List data = channelIdToDataMap.remove(channel.getId());
    // Local hit
    if (data != null) {
      // System.out.println(channel + " Local READ: " + data);
      return data;
    } else {
      final List remoteReadData = remoteRead(channelId);
      // System.out.println(channel + " Remote READ: " + remoteReadData);
      return remoteReadData;
    }
  }

  // On out-channel
  void onReadRequest(String executorId, String channelId) {
    Connection<DataMessage> conn = getConnectionFactory()
        .newConnection(idFactory.getNewInstance(executorId));

    List data = channelIdToDataMap.remove(channelId);
    if (data == null) {
      final String errMessage = "Read on empty channel " + channelId;
      LOG.log(Level.WARNING, errMessage);
      // throw new RuntimeException(errMessage);
      data = new ArrayList(0);
    }
    try {
      conn.open();
      conn.write(new DataMessage(channelId, data));
    } catch (final NetworkException e) {
      final String errMessage = "NetworkException occurred!" + e.getMessage();
      LOG.log(Level.WARNING, errMessage);
      throw new RuntimeException(errMessage, e);
    }
  }

  public void sendChannelReady(final String channelId) {
    vortexExecutor.sendChannelReady(channelId);
  }

  // On in-channel
  private List remoteRead(final String channelId) {
    final CountDownLatch latch = new CountDownLatch(1);
    System.out.println("Wait for channel " + channelId);

    latchMap.put(channelId, latch);
    vortexExecutor.sendReadRequest(channelId);

    // Wait for message arrives
    try {
      latch.await();
    } catch (final InterruptedException e) {
      final String errMessage = "InterruptedException occurred!" + e.getMessage();
      LOG.log(Level.WARNING, errMessage);
      throw new RuntimeException(errMessage, e);
    }

    final List readData = channelIdToDataMap.remove(channelId);
    if (readData == null) {
      return new ArrayList<>();
    }

    return readData;
  }

  public static BlockManager getInstance() {
    return instance;
  }
}
