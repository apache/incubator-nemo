package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.HEARTBEAT;
import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.KAFKA_CHECKPOINT;
import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.OFFLOADING_RESULT;

public final class MiddleOffloadingOutputEncoder implements OffloadingEncoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputEncoder.class.getName());

  private final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;

  public MiddleOffloadingOutputEncoder(final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder) {
    this.checkpointMarkCoder = checkpointMarkCoder;
  }

  @Override
  public void encode(Object data, OutputStream outputStream) throws IOException {

    if (data instanceof OffloadingResultTimestampEvent) {
      final OffloadingResultTimestampEvent element = (OffloadingResultTimestampEvent) data;
      //LOG.info("Encode elment: {}, {}, {}", element.vertexId, element.timestamp, element.watermark);
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(OFFLOADING_RESULT);
      dos.writeUTF(element.taskId);
      dos.writeUTF(element.vertexId);
      dos.writeLong(element.timestamp);
      dos.writeLong(element.watermark);
    } else if (data instanceof OffloadingHeartbeatEvent) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      final OffloadingHeartbeatEvent element = (OffloadingHeartbeatEvent) data;
      dos.writeChar(HEARTBEAT);
      dos.writeUTF(element.taskId);
      dos.writeInt(element.taskIndex);
      dos.writeLong(element.time);
    } else if (data instanceof KafkaOffloadingOutput) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(KAFKA_CHECKPOINT);
      final KafkaOffloadingOutput output = (KafkaOffloadingOutput) data;
      dos.writeUTF(output.taskId);
      dos.writeInt(output.id);
      checkpointMarkCoder.encode(output.checkpointMark, outputStream);
    }
  }
}
