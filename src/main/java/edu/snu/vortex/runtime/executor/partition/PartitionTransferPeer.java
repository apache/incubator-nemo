/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.runtime.executor.partition;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.NodeConnectionException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionStoreException;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.Codec;
import org.apache.reef.wake.remote.impl.TransportEvent;
import org.apache.reef.wake.remote.transport.Link;
import org.apache.reef.wake.remote.transport.LinkListener;
import org.apache.reef.wake.remote.transport.Transport;
import org.apache.reef.wake.remote.transport.TransportFactory;
import org.apache.reef.wake.remote.transport.netty.LoggingLinkListener;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles partition transfer between {@link edu.snu.vortex.runtime.executor.Executor}s.
 */
@ThreadSafe
final class PartitionTransferPeer {
  private static final Logger LOG = Logger.getLogger(PartitionTransferPeer.class.getName());
  private static final RequestPartitionMessageCodec REQUEST_MESSAGE_CODEC = new RequestPartitionMessageCodec();
  private static final LinkListener LINK_LISTENER = new LoggingLinkListener();

  private final Transport transport;
  private final NameResolver nameResolver;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  private final AtomicLong requestIdCounter;
  private final ConcurrentHashMap<Long, Coder> requestIdToCoder;
  private final ConcurrentHashMap<Long, CompletableFuture<Iterable<Element>>> requestIdToFuture;

  @Inject
  private PartitionTransferPeer(final NameResolver nameResolver,
                                final TransportFactory transportFactory,
                                final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
                                final PartitionClientHandler partitionClientHandler,
                                final PartitionServerHandler partitionServerHandler,
                                final ExceptionHandler exceptionHandler,
                                @Parameter(JobConf.ExecutorId.class) final String executorId) {
    this.nameResolver = nameResolver;
    this.partitionManagerWorker = partitionManagerWorker;
    this.requestIdCounter = new AtomicLong(1);
    this.requestIdToCoder = new ConcurrentHashMap<>();
    this.requestIdToFuture = new ConcurrentHashMap<>();

    transport = transportFactory.newInstance(0, partitionClientHandler, partitionServerHandler, exceptionHandler);
    final InetSocketAddress serverAddress = (InetSocketAddress) transport.getLocalAddress();
    LOG.log(Level.FINE, "PartitionTransferPeer starting, listening at {0}", serverAddress);

    final Identifier serverIdentifier = new PartitionTransferPeerIdentifier(executorId);
    try {
      nameResolver.register(serverIdentifier, serverAddress);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Cannot register PartitionTransferPeer to name server");
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches a partition asynchronously.
   * @param remoteExecutorId id of the remote executor
   * @param partitionId id of the partition
   * @param runtimeEdgeId id of the {@link edu.snu.vortex.runtime.common.plan.RuntimeEdge} corresponds to the partition
   * @param partitionStore type of the partition store
   * @return {@link CompletableFuture} for the partition
   */
  CompletableFuture<Iterable<Element>> fetch(final String remoteExecutorId,
                                             final String partitionId,
                                             final String runtimeEdgeId,
                                             final RuntimeAttribute partitionStore) {
    final Identifier remotePeerIdentifier = new PartitionTransferPeerIdentifier(remoteExecutorId);
    final InetSocketAddress remoteAddress;
    final Coder coder = partitionManagerWorker.get().getCoder(runtimeEdgeId);
    try {
      remoteAddress = nameResolver.lookup(remotePeerIdentifier);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Cannot lookup PartitionTransferPeer {0}", remotePeerIdentifier);
      throw new NodeConnectionException(e);
    }
    LOG.log(Level.INFO, "Looked up {0}", remoteAddress);

    final Link<ControlMessage.RequestPartitionMsg> link;
    try {
      link = transport.open(remoteAddress, REQUEST_MESSAGE_CODEC, LINK_LISTENER);
    } catch (final IOException e) {
      throw new NodeConnectionException(e);
    }
    final long requestId = requestIdCounter.getAndIncrement();
    final CompletableFuture<Iterable<Element>> future = new CompletableFuture<>();
    requestIdToCoder.put(requestId, coder);
    requestIdToFuture.put(requestId, future);
    final ControlMessage.RequestPartitionMsg msg = ControlMessage.RequestPartitionMsg.newBuilder()
        .setRequestId(requestId)
        .setPartitionId(partitionId)
        .setRuntimeEdgeId(runtimeEdgeId)
        .setPartitionStore(convertPartitionStore(partitionStore))
        .build();
    link.write(msg);

    LOG.log(Level.INFO, "Wrote request {0}", msg);
    return future;
  }

  /**
   * A {@link Codec} implementation for {@link ControlMessage.RequestPartitionMsg}.
   */
  private static final class RequestPartitionMessageCodec implements Codec<ControlMessage.RequestPartitionMsg> {
    @Override
    public byte[] encode(final ControlMessage.RequestPartitionMsg msg) {
      return msg.toByteArray();
    }

    @Override
    public ControlMessage.RequestPartitionMsg decode(final byte[] bytes) {
      try {
        return ControlMessage.RequestPartitionMsg.parseFrom(bytes);
      } catch (final InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * An {@link EventHandler} for incoming requests for {@link PartitionTransferPeer}.
   */
  private static final class PartitionServerHandler implements EventHandler<TransportEvent> {
    private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

    @Inject
    private PartitionServerHandler(final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
      this.partitionManagerWorker = partitionManagerWorker;
    }

    @Override
    public void onNext(final TransportEvent transportEvent) {
      final PartitionManagerWorker worker = partitionManagerWorker.get();
      final ControlMessage.RequestPartitionMsg request = REQUEST_MESSAGE_CODEC.decode(transportEvent.getData());

      // We are getting the partition from local store!
      final Iterable<Element> data = worker.getPartition(request.getPartitionId(), request.getRuntimeEdgeId(),
          convertPartitionStoreType(request.getPartitionStore()));

      final Coder coder = worker.getCoder(request.getRuntimeEdgeId());
      final ControlMessage.PartitionTransferMsg.Builder replyBuilder = ControlMessage.PartitionTransferMsg.newBuilder()
          .setRequestId(request.getRequestId());
      for (final Element element : data) {
        // Memory leak if we don't do try-with-resources here
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
          coder.encode(element, stream);
          replyBuilder.addData(ByteString.copyFrom(stream.toByteArray()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      final byte[] serialized = replyBuilder.build().toByteArray();
      transportEvent.getLink().write(serialized);
    }
  }

  /**
   * An {@link EventHandler} for response from {@link PartitionServerHandler}.
   */
  private static final class PartitionClientHandler implements EventHandler<TransportEvent> {
    private final InjectionFuture<PartitionTransferPeer> partitionTransferPeer;

    @Inject
    private PartitionClientHandler(final InjectionFuture<PartitionTransferPeer> partitionTransferPeer) {
      this.partitionTransferPeer = partitionTransferPeer;
    }

    @Override
    public void onNext(final TransportEvent transportEvent) {
      final PartitionTransferPeer peer = partitionTransferPeer.get();
      final ControlMessage.PartitionTransferMsg reply;
      try {
        reply = ControlMessage.PartitionTransferMsg.parseFrom(transportEvent.getData());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      final Coder coder = peer.requestIdToCoder.remove(reply.getRequestId());
      final ArrayList<Element> deserializedData = new ArrayList<>(reply.getDataCount());
      for (int i = 0; i < reply.getDataCount(); i++) {
        // Memory leak if we don't do try-with-resources here
        try (final InputStream inputStream = reply.getData(i).newInput()) {
          deserializedData.add(coder.decode(inputStream));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      final CompletableFuture<Iterable<Element>> future = peer.requestIdToFuture.remove(reply.getRequestId());
      future.complete(deserializedData);
    }
  }

  /**
   * An {@link EventHandler} for {@link Exception}s during partition transfer.
   */
  private static final class ExceptionHandler implements EventHandler<Exception> {
    @Inject
    private ExceptionHandler() {
    }

    @Override
    public void onNext(final Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  private static ControlMessage.PartitionStore convertPartitionStore(final RuntimeAttribute partitionStore) {
    switch (partitionStore) {
      case Local:
        return ControlMessage.PartitionStore.LOCAL;
      case Memory:
        // TODO #181: Implement MemoryPartitionStore
        return ControlMessage.PartitionStore.MEMORY;
      case File:
        // TODO #69: Implement file channel in Runtime
        return ControlMessage.PartitionStore.FILE;
      case MemoryFile:
        // TODO #69: Implement file channel in Runtime
        return ControlMessage.PartitionStore.MEMORY_FILE;
      case DistributedStorage:
        // TODO #180: Implement DistributedStorageStore
        return ControlMessage.PartitionStore.DISTRIBUTED_STORAGE;
      default:
        throw new UnsupportedPartitionStoreException(new Exception(partitionStore + " is not supported."));
    }
  }

  private static RuntimeAttribute convertPartitionStoreType(final ControlMessage.PartitionStore partitionStoreType) {
    switch (partitionStoreType) {
      case LOCAL:
        return RuntimeAttribute.Local;
      case MEMORY:
        return RuntimeAttribute.Memory;
      case FILE:
        return RuntimeAttribute.File;
      case MEMORY_FILE:
        return RuntimeAttribute.MemoryFile;
      case DISTRIBUTED_STORAGE:
        return RuntimeAttribute.DistributedStorage;
      default:
        throw new UnsupportedPartitionStoreException(new Throwable("This partition store is not yet supported"));
    }
  }
}
