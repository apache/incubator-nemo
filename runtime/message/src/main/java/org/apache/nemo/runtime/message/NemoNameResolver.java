package org.apache.nemo.runtime.message;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.NAMING_REQUEST_ID;

public final class NemoNameResolver {
  private static final Logger LOG = LoggerFactory.getLogger(NemoNameResolver.class);

  private final MessageSender nameResolver;

  @Inject
  private NemoNameResolver(
    @Parameter(MessageParameters.NameServerPort.class) final int nameServerPort,
    @Parameter(MessageParameters.NameServerAddr.class) final String nameServerAddr,
    @Parameter(JobConf.ExecutorId.class) final String senderId,
    final MessageEnvironment messageEnvironment) throws ExecutionException, InterruptedException {

    this.nameResolver = messageEnvironment.asyncConnect(
      MessageEnvironment.MASTER_ID, NAMING_REQUEST_ID,
      new InetSocketAddress(nameServerAddr, nameServerPort)).get();
  }

  public void register(final String identifier, InetSocketAddress addr) {

    LOG.info("Registering {}", identifier);

    nameResolver.send(ControlMessage.Message.newBuilder()
      .setId(RuntimeIdManager.generateMessageId())
      .setListenerId(NAMING_REQUEST_ID.ordinal())
      .setType(ControlMessage.MessageType.NameLookup)
      .setNameRegisterMsg(ControlMessage.NameRegisterMessage.newBuilder()
        .setIdentifier(identifier)
        .setInet(ByteString.copyFrom(SerializationUtils.serialize(addr)))
        .build())
      .build());
  }

  public InetSocketAddress lookup(final String identifier) {

    LOG.info("Lookup {}", identifier);

    final CompletableFuture future = nameResolver.request(ControlMessage.Message.newBuilder()
      .setId(RuntimeIdManager.generateMessageId())
      .setListenerId(NAMING_REQUEST_ID.ordinal())
      .setType(ControlMessage.MessageType.NameLookup)
      .setNameLookupMsg(ControlMessage.NameLookupMessage.newBuilder()
        .setIdentifier(identifier)
        .build())
      .build());

    try {
      final ControlMessage.Message msg = (ControlMessage.Message) future.get();
      final InetSocketAddress addr =
        SerializationUtils.deserialize(msg.getNameLookupResponse().getInet().toByteArray());
      return addr;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
