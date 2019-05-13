package org.apache.nemo.runtime.lambdaexecutor.general;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;

import org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

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
  public final UnboundedSource unboundedSource;

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
                        final UnboundedSource unboundedSource) {
    this.executorId = executorId;
    this.taskId = taskId;
    this.taskIndex = taskIndex;
    this.irDag = irDag;
    this.samplingMap = samplingMap;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
    this.checkpointMark = checkpointMark;
    this.unboundedSource = unboundedSource;
  }

  public ByteBuf encode(final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    try {

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

      final ObjectOutputStream oos = new ObjectOutputStream(bos);

      oos.writeObject(samplingMap);
      oos.writeObject(irDag);
      oos.writeObject(taskOutgoingEdges);
      oos.writeObject(outgoingEdges);
      oos.writeObject(incomingEdges);

      if (checkpointMark != null) {
        dos.writeBoolean(true);
        checkpointMarkCoder.encode(checkpointMark, bos);
        SerializationUtils.serialize(unboundedSource, bos);
      } else {
        dos.writeBoolean(false);
      }

      dos.close();
      oos.close();
      bos.close();

      //final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
      //final ByteBufOutputStream bbos = new ByteBufOutputStream(byteBuf);
      //final byte[] barray = bos.toByteArray();
      //bbos.write(barray);

      //bbos.close();

      LOG.info("Encoded size: {}, taskOrdinal: {}", byteBuf.readableBytes(), TASK_START.ordinal());
      //LOG.info("Byte array logging");
      //LOG.info(Arrays.toString(barray));

      return byteBuf;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static OffloadingTask decode(
    final InputStream inputStream,
    final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {

    try {

      final DataInputStream dis = new DataInputStream(inputStream);
      final String executorId = dis.readUTF();
      final String taskId = dis.readUTF();
      final int taskIndex = dis.readInt();

      final ObjectInputStream ois = new ObjectInputStream(inputStream);
      final Map<String, Double> samplingMap = (Map<String, Double>) ois.readObject();
      LOG.info("{}, samplingMap: {}", taskId, samplingMap);
      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag = (DAG<IRVertex, RuntimeEdge<IRVertex>> ) ois.readObject();
      final Map<String, List<String>> taskOutgoingEdges = (Map<String, List<String>>) ois.readObject();
      LOG.info("{}, taskOutgoingEdges: {}", taskId, taskOutgoingEdges);
      final List<StageEdge> outgoingEdges = (List<StageEdge>) ois.readObject();
      LOG.info("{}, outgoingEdges: {}", taskId, outgoingEdges);
      final List<StageEdge> incomingEdges = (List<StageEdge>) ois.readObject();
      LOG.info("{}, incomingEdges: {}", taskId, incomingEdges);

      final UnboundedSource.CheckpointMark checkpointMark;
      final UnboundedSource unboundedSource;
      final boolean hasCheckpoint = dis.readBoolean();
      if (hasCheckpoint) {
        checkpointMark = checkpointMarkCoder.decode(inputStream);
        unboundedSource = SerializationUtils.deserialize(inputStream);
      } else {
        checkpointMark = null;
        unboundedSource = null;
      }

      return new OffloadingTask(executorId,
        taskId,
        taskIndex,
        samplingMap,
        irDag,
        taskOutgoingEdges,
        outgoingEdges,
        incomingEdges,
        checkpointMark,
        unboundedSource);

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


  }
}
