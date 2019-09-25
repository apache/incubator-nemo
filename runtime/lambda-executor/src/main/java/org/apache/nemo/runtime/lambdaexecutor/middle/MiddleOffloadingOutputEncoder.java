package org.apache.nemo.runtime.lambdaexecutor.middle;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.offloading.common.OffloadingEncoder;
import org.apache.nemo.runtime.executor.common.OffloadingDoneEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultTimestampEvent;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.ThpEvent;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutputEncoder.*;

public final class MiddleOffloadingOutputEncoder implements OffloadingEncoder<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(MiddleOffloadingOutputEncoder.class.getName());


  public MiddleOffloadingOutputEncoder() {
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
    } else if (data instanceof ThpEvent) {

      final ThpEvent element = (ThpEvent) data;
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(THP);
      dos.writeUTF(element.taskId);
      dos.writeUTF(element.opId);
      dos.writeLong(element.thp);

    } else if (data instanceof OffloadingHeartbeatEvent) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      final OffloadingHeartbeatEvent element = (OffloadingHeartbeatEvent) data;
      dos.writeChar(HEARTBEAT);
      dos.writeInt(element.taskMetrics.size());

      for (final Pair<String, TaskMetrics.RetrievedMetrics> taskMetric : element.taskMetrics) {
        dos.writeUTF(taskMetric.left());
        dos.writeLong(taskMetric.right().inputElement);
        dos.writeLong(taskMetric.right().outputElement);
        dos.writeLong(taskMetric.right().computation);
        dos.writeInt(taskMetric.right().numKeys);
      }

    } else if (data instanceof KafkaOffloadingOutput) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(KAFKA_CHECKPOINT);
      final KafkaOffloadingOutput output = (KafkaOffloadingOutput) data;
      dos.writeUTF(output.taskId);
      dos.writeInt(output.id);
      SerializationUtils.serialize(output.checkpointMarkCoder, dos);
      output.checkpointMarkCoder.encode(output.checkpointMark, outputStream);

      if (output.stateMap != null && !output.stateMap.isEmpty()) {
        dos.writeInt(output.stateMap.size());
        for (final Map.Entry<String, GBKFinalState> entry : output.stateMap.entrySet()) {
          final Coder<GBKFinalState> stateCoder = output.stateCoderMap.get(entry.getKey());
          dos.writeUTF(entry.getKey());
          SerializationUtils.serialize(stateCoder, outputStream);
          stateCoder.encode(entry.getValue(), outputStream);
        }
      } else {
        dos.writeInt(0);
      }

      LOG.info("End of encoding state output {}", output.taskId);

    } else if (data instanceof StateOutput) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(STATE_OUTPUT);
      final StateOutput output = (StateOutput) data;
      dos.writeUTF(output.taskId);

      if (output.stateMap != null && !output.stateMap.isEmpty()) {
        dos.writeInt(output.stateMap.size());
         for (final Map.Entry<String, GBKFinalState> entry : output.stateMap.entrySet()) {
           final Coder<GBKFinalState> stateCoder = output.stateCoderMap.get(entry.getKey());
           dos.writeUTF(entry.getKey());
           SerializationUtils.serialize(stateCoder, outputStream);
           stateCoder.encode(entry.getValue(), outputStream);
         }
      } else {
        dos.writeInt(0);
      }
    } else if (data instanceof OffloadingDoneEvent) {
      final DataOutputStream dos = new DataOutputStream(outputStream);
      dos.writeChar(OFFLOADING_DONE);
      final OffloadingDoneEvent output = (OffloadingDoneEvent) data;
      dos.writeUTF(output.taskId);
    }
  }
}
