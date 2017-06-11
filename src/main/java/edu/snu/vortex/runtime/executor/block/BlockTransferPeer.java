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
package edu.snu.vortex.runtime.executor.block;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.UnsupportedBlockStoreException;
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
 * Handles block transfer between {@link edu.snu.vortex.runtime.executor.Executor}s.
 */
@ThreadSafe
final class BlockTransferPeer {
  private static final Logger LOG = Logger.getLogger(BlockTransferPeer.class.getName());
  private static final RequestBlockMessageCodec REQUEST_MESSAGE_CODEC = new RequestBlockMessageCodec();
  private static final LinkListener LINK_LISTENER = new LoggingLinkListener();

  private final Transport transport;
  private final NameResolver nameResolver;
  private final InjectionFuture<BlockManagerWorker> blockManagerWorker;

  private final AtomicLong requestIdCounter;
  private final ConcurrentHashMap<Long, Coder> requestIdToCoder;
  private final ConcurrentHashMap<Long, CompletableFuture<Iterable<Element>>> requestIdToFuture;

  @Inject
  private BlockTransferPeer(final NameResolver nameResolver,
                            final TransportFactory transportFactory,
                            final InjectionFuture<BlockManagerWorker> blockManagerWorker,
                            final BlockClientHandler blockClientHandler,
                            final BlockServerHandler blockServerHandler,
                            final ExceptionHandler exceptionHandler,
                            @Parameter(JobConf.ExecutorId.class) final String executorId) {
    this.nameResolver = nameResolver;
    this.blockManagerWorker = blockManagerWorker;
    this.requestIdCounter = new AtomicLong(1);
    this.requestIdToCoder = new ConcurrentHashMap<>();
    this.requestIdToFuture = new ConcurrentHashMap<>();

    transport = transportFactory.newInstance(0, blockClientHandler, blockServerHandler, exceptionHandler);
    final InetSocketAddress serverAddress = (InetSocketAddress) transport.getLocalAddress();
    LOG.log(Level.FINE, "BlockTransferPeer starting, listening at {0}", serverAddress);

    final Identifier serverIdentifier = new BlockTransferPeerIdentifier(executorId);
    try {
      nameResolver.register(serverIdentifier, serverAddress);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Cannot register BlockTransferPeer to name server");
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches a block asynchronously.
   * @param remoteExecutorId id of the remote executor
   * @param blockId id of the block
   * @param runtimeEdgeId id of the {@link edu.snu.vortex.runtime.common.plan.RuntimeEdge} corresponds to the block
   * @param blockStore type of the block store
   * @return {@link CompletableFuture} for the block
   */
  CompletableFuture<Iterable<Element>> fetch(final String remoteExecutorId,
                                             final String blockId,
                                             final String runtimeEdgeId,
                                             final RuntimeAttribute blockStore) {
    final Identifier remotePeerIdentifier = new BlockTransferPeerIdentifier(remoteExecutorId);
    final InetSocketAddress remoteAddress;
    final Coder coder = blockManagerWorker.get().getCoder(runtimeEdgeId);
    try {
      remoteAddress = nameResolver.lookup(remotePeerIdentifier);
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Cannot lookup BlockTransferPeer {0}", remotePeerIdentifier);
      throw new RuntimeException(e);
    }
    LOG.log(Level.INFO, "Looked up {0}", remoteAddress);

    final Link<ControlMessage.RequestBlockMsg> link;
    try {
      link = transport.open(remoteAddress, REQUEST_MESSAGE_CODEC, LINK_LISTENER);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final long requestId = requestIdCounter.getAndIncrement();
    final CompletableFuture<Iterable<Element>> future = new CompletableFuture<>();
    requestIdToCoder.put(requestId, coder);
    requestIdToFuture.put(requestId, future);
    final ControlMessage.RequestBlockMsg msg = ControlMessage.RequestBlockMsg.newBuilder()
        .setRequestId(requestId)
        .setBlockId(blockId)
        .setRuntimeEdgeId(runtimeEdgeId)
        .setBlockStore(convertBlockStore(blockStore))
        .build();
    link.write(msg);

    LOG.log(Level.INFO, "Wrote request {0}", msg);
    return future;
  }

  /**
   * A {@link Codec} implementation for {@link ControlMessage.RequestBlockMsg}.
   */
  private static final class RequestBlockMessageCodec implements Codec<ControlMessage.RequestBlockMsg> {
    @Override
    public byte[] encode(final ControlMessage.RequestBlockMsg msg) {
      return msg.toByteArray();
    }

    @Override
    public ControlMessage.RequestBlockMsg decode(final byte[] bytes) {
      try {
        return ControlMessage.RequestBlockMsg.parseFrom(bytes);
      } catch (final InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * An {@link EventHandler} for incoming requests for {@link BlockTransferPeer}.
   */
  private static final class BlockServerHandler implements EventHandler<TransportEvent> {
    private final InjectionFuture<BlockManagerWorker> blockManagerWorker;

    @Inject
    private BlockServerHandler(final InjectionFuture<BlockManagerWorker> blockManagerWorker) {
      this.blockManagerWorker = blockManagerWorker;
    }

    @Override
    public void onNext(final TransportEvent transportEvent) {
      final BlockManagerWorker worker = blockManagerWorker.get();
      final ControlMessage.RequestBlockMsg request = REQUEST_MESSAGE_CODEC.decode(transportEvent.getData());
      final Iterable<Element> data = worker.getBlock(request.getBlockId(), request.getRuntimeEdgeId(),
          convertBlockStoreType(request.getBlockStore()));
      final Coder coder = worker.getCoder(request.getRuntimeEdgeId());
      final ControlMessage.BlockTransferMsg.Builder replyBuilder = ControlMessage.BlockTransferMsg.newBuilder()
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
   * An {@link EventHandler} for response from {@link BlockServerHandler}.
   */
  private static final class BlockClientHandler implements EventHandler<TransportEvent> {
    private final InjectionFuture<BlockTransferPeer> blockTransferPeer;

    @Inject
    private BlockClientHandler(final InjectionFuture<BlockTransferPeer> blockTransferPeer) {
      this.blockTransferPeer = blockTransferPeer;
    }

    @Override
    public void onNext(final TransportEvent transportEvent) {
      final BlockTransferPeer peer = blockTransferPeer.get();
      final ControlMessage.BlockTransferMsg reply;
      try {
        reply = ControlMessage.BlockTransferMsg.parseFrom(transportEvent.getData());
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
   * An {@link EventHandler} for {@link Exception}s during block transfer.
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

  private static ControlMessage.BlockStore convertBlockStore(final RuntimeAttribute blockStore) {
    switch (blockStore) {
      case Local:
        return ControlMessage.BlockStore.LOCAL;
      case Memory:
        // TODO #181: Implement MemoryBlockStore
        return ControlMessage.BlockStore.MEMORY;
      case File:
        // TODO #69: Implement file channel in Runtime
        return ControlMessage.BlockStore.FILE;
      case MemoryFile:
        // TODO #69: Implement file channel in Runtime
        return ControlMessage.BlockStore.MEMORY_FILE;
      case DistributedStorage:
        // TODO #180: Implement DistributedStorageStore
        return ControlMessage.BlockStore.DISTRIBUTED_STORAGE;
      default:
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }

  private static RuntimeAttribute convertBlockStoreType(final ControlMessage.BlockStore blockStoreType) {
    switch (blockStoreType) {
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
        throw new UnsupportedBlockStoreException(new Throwable("This block store is not yet supported"));
    }
  }
}
