package org.apache.nemo.runtime.lambdaexecutor.general;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;

import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;


public final class OffloadingTask {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadingTask.class.getName());

  public final String executorId;
  public final int taskIndex;
  public final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  public final Map<String, List<String>> taskOutgoingEdges;

  // next stage address
  public final Map<String, Double> samplingMap;

  public final List<StageEdge> outgoingEdges;
  public final List<StageEdge> incomingEdges;

  public String taskId;
  public final UnboundedSource.CheckpointMark checkpointMark;
  public final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;
  public final UnboundedSource unboundedSource;
  public final Map<String, GBKFinalState> stateMap;
  public final Map<String, Coder<GBKFinalState>> stateCoderMap;
  public final long prevWatermarkTimestamp;

  // TODO: we should get checkpoint mark in constructor!
  public OffloadingTask(final String executorId,
                        final String taskId,
                        final int taskIndex,
                        final Map<String, Double> samplingMap,
                        final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                        final Map<String, List<String>> taskOutgoingEdges,
                        final List<StageEdge> outgoingEdges,
                        final List<StageEdge> incomingEdges,
                        final UnboundedSource.CheckpointMark checkpointMark,
                        final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder,
                        final long prevWatermarkTimestamp,
                        final UnboundedSource unboundedSource,
                        final Map<String, GBKFinalState>  stateMap,
                        final Map<String, Coder<GBKFinalState>> stateCoderMap) {
    this.executorId = executorId;
    this.taskId = taskId;
    this.taskIndex = taskIndex;
    this.irDag = irDag;
    this.samplingMap = samplingMap;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
    this.checkpointMark = checkpointMark;
    this.prevWatermarkTimestamp = prevWatermarkTimestamp;
    this.checkpointMarkCoder = checkpointMarkCoder;
    this.unboundedSource = unboundedSource;
    this.stateMap = stateMap;
    this.stateCoderMap = stateCoderMap;
  }

  public ByteBuf encode() {
    try {

      final FSTConfiguration conf = FSTSingleton.getInstance();

      //final ByteArrayOutputStream bos = new ByteArrayOutputStream(172476);

      //LOG.info("Before Task ordinal11 !!");
      //LOG.info(Arrays.toString(bos.toByteArray()));

      final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
      final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

      final DataOutputStream dos = new DataOutputStream(bos);

      //LOG.info("Before Task ordinal !!");
      //LOG.info(Arrays.toString(bos.toByteArray()));

      dos.writeInt(TASK_START.ordinal());

      //LOG.info("Task ordinal !!");
      //LOG.info(Arrays.toString(bos.toByteArray()));

      dos.writeUTF(executorId);
      dos.writeUTF(taskId);
      dos.writeInt(taskIndex);

      conf.encodeToStream(bos, samplingMap);
      conf.encodeToStream(bos, taskOutgoingEdges);
      conf.encodeToStream(bos, outgoingEdges);
      conf.encodeToStream(bos, incomingEdges);
      conf.encodeToStream(bos, irDag);

      /*
      final ObjectOutputStream oos = new ObjectOutputStream(bos);

      oos.writeObject(samplingMap);
      oos.writeObject(irDag);
      oos.writeObject(taskOutgoingEdges);
      oos.writeObject(outgoingEdges);
      oos.writeObject(incomingEdges);
      */

      if (checkpointMark != null) {
        dos.writeBoolean(true);
        conf.encodeToStream(bos, checkpointMarkCoder);
        checkpointMarkCoder.encode(checkpointMark, bos);
        conf.encodeToStream(bos, unboundedSource);
        dos.writeLong(prevWatermarkTimestamp);
      } else {
        dos.writeBoolean(false);
      }

      if (stateCoderMap != null && !stateCoderMap.isEmpty()) {
        dos.writeInt(stateMap.size());
        for (final Map.Entry<String, GBKFinalState> vertexIdAndState : stateMap.entrySet()) {
          dos.writeUTF(vertexIdAndState.getKey());
          conf.encodeToStream(bos, stateCoderMap.get(vertexIdAndState.getKey()));
          stateCoderMap.get(vertexIdAndState.getKey()).encode(vertexIdAndState.getValue(), bos);
        }

      } else {
        dos.writeInt(0);
      }

      dos.close();
      //oos.close();
      bos.close();

      LOG.info("Encoding state done for {}, size: {}", taskId, byteBuf.readableBytes());

      return byteBuf;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static OffloadingTask decode(
    final InputStream inputStream) {

    try {

      final FSTConfiguration conf = FSTSingleton.getInstance();

      final DataInputStream dis = new DataInputStream(inputStream);
      final String executorId = dis.readUTF();
      final String taskId = dis.readUTF();
      final int taskIndex = dis.readInt();

      LOG.info("Decoding task!! {}", taskId);

      final Map<String, Double> samplingMap = (Map<String, Double>) conf.decodeFromStream(inputStream);
      LOG.info("{}, samplingMap: {}", taskId, samplingMap);

      final Map<String, List<String>> taskOutgoingEdges =  (Map<String, List<String>>) conf.decodeFromStream(inputStream);
      LOG.info("{}, taskOutgoingEdges: {}", taskId, taskOutgoingEdges);
      final List<StageEdge> outgoingEdges = (List<StageEdge>) conf.decodeFromStream(inputStream);
      LOG.info("{}, outgoingEdges: {}", taskId, outgoingEdges);
      final List<StageEdge> incomingEdges = (List<StageEdge>) conf.decodeFromStream(inputStream);
      LOG.info("{}, incomingEdges: {}", taskId, incomingEdges);

      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
        (DAG<IRVertex, RuntimeEdge<IRVertex>>) conf.decodeFromStream(inputStream);
      LOG.info("{}, irDag: {}", taskId, irDag);

      final UnboundedSource.CheckpointMark checkpointMark;
      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;
      final UnboundedSource unboundedSource;
      final long prevWatermarkTimestamp;
      final boolean hasCheckpoint = dis.readBoolean();
      if (hasCheckpoint) {
        checkpointMarkCoder = (Coder<UnboundedSource.CheckpointMark>) conf.decodeFromStream(inputStream);
        checkpointMark = checkpointMarkCoder.decode(inputStream);
        unboundedSource = (UnboundedSource) conf.decodeFromStream(inputStream);
        prevWatermarkTimestamp  = dis.readLong();
      } else {
        checkpointMark = null;
        unboundedSource = null;
        checkpointMarkCoder = null;
        prevWatermarkTimestamp = -1;
      }

      final Map<String, GBKFinalState> stateMap = new HashMap<>();
      final Map<String, Coder<GBKFinalState>> stateCoderMap = new HashMap<>();
      final int size = dis.readInt();
      for (int i = 0; i < size; i++) {
        final String key = dis.readUTF();
        final Coder<GBKFinalState> stateCoder = (Coder<GBKFinalState>) conf.decodeFromStream(inputStream);
        final GBKFinalState state = stateCoder.decode(inputStream);
        stateMap.put(key, state);
        stateCoderMap.put(key, stateCoder);
      }

      LOG.info("Decoding state {}", taskId);

      return new OffloadingTask(executorId,
        taskId,
        taskIndex,
        samplingMap,
        irDag,
        taskOutgoingEdges,
        outgoingEdges,
        incomingEdges,
        checkpointMark,
        checkpointMarkCoder,
        prevWatermarkTimestamp,
        unboundedSource,
        stateMap,
        stateCoderMap);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


  }
}
