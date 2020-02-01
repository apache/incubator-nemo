package org.apache.nemo.runtime.lambdaexecutor.middle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.OffloadingDoneEvent;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.*;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingOutputEncoder.*;


public final class MiddleOffloadingOutputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputDecoder.class.getName());


  public MiddleOffloadingOutputDecoder() {
  }

  @Override
  public Object decode(InputStream inputStream) throws IOException {
    throw new UnsupportedOperationException("No support");
  }

  @Override
  public Object decode(ByteBuf byteBuf) throws IOException {
    final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
    final DataInputStream dis = new DataInputStream(bis);
    final char type = dis.readChar();

    switch (type) {
      case OFFLOADING_RESULT: {
        final String taskId = dis.readUTF();
        final String vertexId = dis.readUTF();
        final long timestamp = dis.readLong();
        final long watermark = dis.readLong();
        //LOG.info("Decode element: {}, {}, {}", vertexId, timestamp, watermark);
        return Pair.of(taskId,
          new OffloadingResultTimestampEvent(taskId, vertexId, timestamp, watermark));
      }
      case THP: {
        final String taskId = dis.readUTF();
        final String opId = dis.readUTF();
        final long thp = dis.readLong();
        return Pair.of(taskId,
          new ThpEvent(taskId, opId, thp));
      }
      case HEARTBEAT: {
        final String executorId = dis.readUTF();
        final int len = dis.readInt();
        final List<Pair<String, TaskMetrics.RetrievedMetrics>> taskMetrics = new ArrayList<>(len);
        String taskId = null;

        for (int i = 0; i < len; i++) {
          taskId = dis.readUTF();
          final long inputElement = dis.readLong();
          final long outputElement = dis.readLong();
          final long computation = dis.readLong();
          final int numKeys = dis.readInt();
          taskMetrics.add(Pair.of(taskId, new TaskMetrics.RetrievedMetrics(
            inputElement, outputElement, computation, numKeys)));
        }

        if (taskId == null) {
          throw new RuntimeException("task id null");
        }

        final double cpuLoad = dis.readDouble();

        return Pair.of(taskId,
          new OffloadingHeartbeatEvent(executorId, taskMetrics, cpuLoad));
      }
      case KAFKA_CHECKPOINT: {
        final boolean moveToVMScaling = dis.readBoolean();

        if (moveToVMScaling) {
          final String taskId = dis.readUTF();

          byteBuf.retain();

          return Pair.of(taskId,
            new KafkaOffloadingOutput(moveToVMScaling,
              taskId, byteBuf));
        } else {

          final String taskId = dis.readUTF();
          final int id = dis.readInt();

          final long st = System.currentTimeMillis();

          final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = SerializationUtils.deserialize(dis);
          final UnboundedSource.CheckpointMark checkpointMark = checkpointMarkCoder.decode(dis);

          final long e = System.currentTimeMillis();
          LOG.info("Checkpoint mark decoding time of {}: {}", taskId, e - st);

          final int mapSize = dis.readInt();
          final Map<String, GBKFinalState> stateMap = new HashMap<>();
          final Map<String, Coder<GBKFinalState>> stateCoderMap = new HashMap<>();
          for (int i = 0; i < mapSize; i++) {
            final String key = dis.readUTF();
            final Coder<GBKFinalState> coder = SerializationUtils.deserialize(dis);
            final GBKFinalState state = coder.decode(dis);
            stateMap.put(key, state);
            stateCoderMap.put(key, coder);
          }

          LOG.info("Map decoding time of {}: {}", taskId, System.currentTimeMillis() - e);

          return Pair.of(taskId,
            new KafkaOffloadingOutput(moveToVMScaling,
              taskId, id, checkpointMark, checkpointMarkCoder, stateMap, stateCoderMap));
        }
      }
      case STATE_OUTPUT: {
        final boolean moveToVmScaling = dis.readBoolean();

        if (moveToVmScaling) {
          final String taskId = dis.readUTF();

          byteBuf.retain();

          return Pair.of(taskId, new StateOutput(moveToVmScaling, taskId, byteBuf));
        } else {
          final String taskId = dis.readUTF();

          final int mapSize = dis.readInt();
          final Map<String, GBKFinalState> stateMap = new HashMap<>();
          final Map<String, Coder<GBKFinalState>> stateCoderMap = new HashMap<>();
          for (int i = 0; i < mapSize; i++) {
            final String key = dis.readUTF();
            final Coder<GBKFinalState> coder = SerializationUtils.deserialize(dis);
            final GBKFinalState state = coder.decode(dis);
            stateMap.put(key, state);
            stateCoderMap.put(key, coder);
          }

          return Pair.of(taskId, new StateOutput(moveToVmScaling, taskId, stateMap, stateCoderMap));
        }
      }
      case OFFLOADING_DONE: {
        final String taskId = dis.readUTF();
        return Pair.of(taskId, new OffloadingDoneEvent(taskId));
      }
      default:
        throw new RuntimeException("Unsupported type: " + type);
    }
  }
}
