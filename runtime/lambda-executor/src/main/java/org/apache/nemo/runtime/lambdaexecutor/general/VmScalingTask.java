package org.apache.nemo.runtime.lambdaexecutor.general;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_START;


public final class VmScalingTask {
  private static final Logger LOG = LoggerFactory.getLogger(VmScalingTask.class.getName());

  public final String executorId;
  public final int taskIndex;
  public final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;
  public final Map<String, List<String>> taskOutgoingEdges;

  // next stage address
  public final Map<String, Double> samplingMap;

  public final List<StageEdge> outgoingEdges;
  public final List<StageEdge> incomingEdges;
  public String taskId;

  // TODO: we should get checkpoint mark in constructor!
  public VmScalingTask(final String executorId,
                       final String taskId,
                       final int taskIndex,
                       final Map<String, Double> samplingMap,
                       final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                       final Map<String, List<String>> taskOutgoingEdges,
                       final List<StageEdge> outgoingEdges,
                       final List<StageEdge> incomingEdges) {
    this.executorId = executorId;
    this.taskId = taskId;
    this.taskIndex = taskIndex;
    this.irDag = irDag;
    this.samplingMap = samplingMap;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
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

      dos.close();
      //oos.close();
      bos.close();


      return byteBuf;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static VmScalingTask decode(
    final InputStream inputStream) {

    try {

      final FSTConfiguration conf = FSTSingleton.getInstance();

      final DataInputStream dis = new DataInputStream(inputStream);
      final String executorId = dis.readUTF();
      final String taskId = dis.readUTF();
      final int taskIndex = dis.readInt();

      LOG.info("Decoding task!! {}", taskId);

      final Map<String, Double> samplingMap = (Map<String, Double>) conf.decodeFromStream(inputStream);
      // LOG.info("{}, samplingMap: {}", taskId, samplingMap);

      final Map<String, List<String>> taskOutgoingEdges =  (Map<String, List<String>>) conf.decodeFromStream(inputStream);
      // LOG.info("{}, taskOutgoingEdges: {}", taskId, taskOutgoingEdges);
      final List<StageEdge> outgoingEdges = (List<StageEdge>) conf.decodeFromStream(inputStream);
      // LOG.info("{}, outgoingEdges: {}", taskId, outgoingEdges);
      final List<StageEdge> incomingEdges = (List<StageEdge>) conf.decodeFromStream(inputStream);
      // LOG.info("{}, incomingEdges: {}", taskId, incomingEdges);

      final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
        (DAG<IRVertex, RuntimeEdge<IRVertex>>) conf.decodeFromStream(inputStream);
      // LOG.info("{}, irDag: {}", taskId, irDag);

      return new VmScalingTask(executorId,
        taskId,
        taskIndex,
        samplingMap,
        irDag,
        taskOutgoingEdges,
        outgoingEdges,
        incomingEdges);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }


  }
}
