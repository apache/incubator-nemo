package org.apache.nemo.runtime.lambdaexecutor.kafka;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.Triple;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public final class KafkaOffloadingOutputEncoder implements OffloadingEncoder<Object> {

  final Map<String, Serializer> serializerMap;

  public static final char OFFLOADING_RESULT = 0;
  public static final char KAFKA_CHECKPOINT = 1;
  public static final char HEARTBEAT = 2;

  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  public KafkaOffloadingOutputEncoder(final Map<String, Serializer> serializerMap,
                                      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    this.serializerMap = serializerMap;
    this.checkpointMarkCoder = checkpointMarkCoder;
  }

  @Override
  public void encode(Object data, OutputStream outputStream) throws IOException {

    if (data instanceof OffloadingHeartbeatEvent) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      final OffloadingHeartbeatEvent element = (OffloadingHeartbeatEvent) data;
      dos.writeChar(HEARTBEAT);
      dos.writeInt(element.taskIndex);
      dos.writeLong(element.time);
    } else if (data instanceof OffloadingResultTimestampEvent) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      final OffloadingResultTimestampEvent element = (OffloadingResultTimestampEvent) data;
      dos.writeChar(OFFLOADING_RESULT);
      dos.writeUTF(element.taskId);
      dos.writeUTF(element.vertexId);
      dos.writeLong(element.timestamp);
      dos.writeLong(element.watermark);
    } else if (data instanceof OffloadingResultEvent) {
      // TODO: unuseed code
      final OffloadingResultEvent element = (OffloadingResultEvent) data;
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(OFFLOADING_RESULT);
      dos.writeInt(element.data.size());
      dos.writeLong(element.watermark);
      //System.out.println("Encoding " + element.data.size() + " events");

      for (final Triple<List<String>, String, Object> triple : element.data) {
        // vertex id
        dos.writeInt(triple.first.size());
        for (final String nextVertexId : triple.first) {
          dos.writeUTF(nextVertexId);
        }
        // edge id
        dos.writeUTF(triple.second);
        final Serializer serializer = serializerMap.get(triple.second);
        serializer.getEncoderFactory().create(outputStream).encode(triple.third);
      }
    } else if (data instanceof KafkaOffloadingOutput) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(KAFKA_CHECKPOINT);
      final KafkaOffloadingOutput output = (KafkaOffloadingOutput) data;
      dos.writeInt(output.id);
      checkpointMarkCoder.encode(output.checkpointMark, outputStream);
    } else {
      throw new RuntimeException("Unsupported type: " + data);
    }
  }
}
