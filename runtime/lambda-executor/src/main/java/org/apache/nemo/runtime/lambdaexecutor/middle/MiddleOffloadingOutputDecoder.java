package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
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

import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.*;

public final class MiddleOffloadingOutputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputDecoder.class.getName());


  public MiddleOffloadingOutputDecoder() {
  }

    @Override
    public Object decode(InputStream inputStream) throws IOException {
      final DataInputStream dis = new DataInputStream(inputStream);
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
        case HEARTBEAT: {
          final String taskId = dis.readUTF();
          final Integer taskIndex = dis.readInt();
          final Long time = dis.readLong();
          return Pair.of(taskId,
            new OffloadingHeartbeatEvent(taskId, taskIndex, time));
        }
        case KAFKA_CHECKPOINT: {
          final String taskId = dis.readUTF();
          final int id = dis.readInt();
          final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = SerializationUtils.deserialize(inputStream);
          final UnboundedSource.CheckpointMark checkpointMark = checkpointMarkCoder.decode(inputStream);

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

          return Pair.of(taskId,
            new KafkaOffloadingOutput(taskId, id, checkpointMark, checkpointMarkCoder, stateMap, stateCoderMap));
        }
        case STATE_OUTPUT: {
          final String  taskId = dis.readUTF();

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

          return Pair.of(taskId, new StateOutput(taskId, stateMap, stateCoderMap));
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
