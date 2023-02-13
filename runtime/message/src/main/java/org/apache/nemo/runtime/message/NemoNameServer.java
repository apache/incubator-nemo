package org.apache.nemo.runtime.message;

import com.google.protobuf.ByteString;
import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.NAMING_REQUEST_ID;

public final class NemoNameServer implements MessageListener<ControlMessage.Message> {
  private static final Logger LOG = LoggerFactory.getLogger(NemoNameServer.class.getName());

  private final Map<String, InetSocketAddress> map = new ConcurrentHashMap<>();
  private final MessageEnvironment messageEnvironment;

  @Inject
  private NemoNameServer(final MessageEnvironment messageEnvironment,
                         final LocalAddressProvider localAddressProvider) {
    map.put(MessageEnvironment.MASTER_ID,
      new InetSocketAddress(localAddressProvider.getLocalAddress(), messageEnvironment.getPort()));

    this.messageEnvironment = messageEnvironment;
    messageEnvironment.setupListener(NAMING_REQUEST_ID, this);
  }

  @Override
  public void onMessage(ControlMessage.Message message) {

    final ControlMessage.NameRegisterMessage msg = message.getNameRegisterMsg();
    LOG.info("Registering {}", msg.getIdentifier());

    map.put(msg.getIdentifier(),
      (InetSocketAddress) SerializationUtils.deserialize(msg.getInet().toByteArray()));
  }

  public int getPort() {
    return messageEnvironment.getPort();
  }

  @Override
  public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {

    final ControlMessage.NameLookupMessage msg = message.getNameLookupMsg();
    final String identifier = msg.getIdentifier();
    LOG.info("Name lookup request {}, addr {}", identifier, map.get(identifier));

    if (map.containsKey(identifier)) {
      final InetSocketAddress inetSocketAddress = map.get(identifier);
      messageContext.reply(ControlMessage.Message.newBuilder()
        .setId(messageContext.getRequestId())
        .setListenerId(NAMING_REQUEST_ID.ordinal())
        .setType(ControlMessage.MessageType.NameLookup)
        .setNameLookupResponse(ControlMessage.NameLookupResponse.newBuilder()
          .setInet(ByteString.copyFrom(SerializationUtils.serialize(inetSocketAddress)))
          .build())
        .build());
    } else {
      throw new RuntimeException("No address " + identifier);
    }
  }
}
