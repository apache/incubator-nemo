package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.Pair;
import org.apache.nemo.offloading.common.OffloadingDecoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.Triple;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOperatorVertexOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.HEARTBEAT;
import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.KAFKA_CHECKPOINT;
import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.OFFLOADING_RESULT;

public final class MiddleOffloadingOutputDecoder implements OffloadingDecoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputDecoder.class.getName());

  private final Coder<UnboundedSource.CheckpointMark> coder;

  public MiddleOffloadingOutputDecoder(final Coder<UnboundedSource.CheckpointMark> coder) {
    this.coder = coder;
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
          final DataInputStream dd = new DataInputStream(inputStream);
          final String taskId = dd.readUTF();
          final int id = dd.readInt();
          final UnboundedSource.CheckpointMark checkpointMark = coder.decode(inputStream);
          return Pair.of(taskId,
            new KafkaOffloadingOutput(taskId, id, checkpointMark));
        }
        default:
          throw new RuntimeException("Unsupported type: " + type);
      }
    }
  }
