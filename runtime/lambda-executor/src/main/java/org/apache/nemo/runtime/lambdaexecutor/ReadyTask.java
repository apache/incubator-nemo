package org.apache.nemo.runtime.lambdaexecutor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.nemo.common.TaskLoc;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nemo.runtime.executor.common.OffloadingExecutorEventType.EventType.TASK_READY;

public final class ReadyTask {
  private static final Logger LOG = LoggerFactory.getLogger(ReadyTask.class.getName());

  public final String taskId;
  public final Map<String, TaskLoc> taskLocationMap;
  public final UnboundedSource.CheckpointMark checkpointMark;
  public final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder;
  public final UnboundedSource unboundedSource;
  public final Map<String, GBKFinalState> stateMap;
  public final Map<String, Coder<GBKFinalState>> stateCoderMap;
  public final long prevWatermarkTimestamp;
  public final Map<String, String> taskExecutorIdMap;

  public ReadyTask(final String taskId,
                   final Map<String, TaskLoc> taskLocationMap,
                   final UnboundedSource.CheckpointMark checkpointMark,
                   final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder,
                   final long prevWatermarkTimestamp,
                   final UnboundedSource unboundedSource,
                   final Map<String, GBKFinalState>  stateMap,
                   final Map<String, Coder<GBKFinalState>> stateCoderMap,
                   final Map<String, String> taskExecutorIdMap) {
    this.taskId = taskId;
    this.taskLocationMap = taskLocationMap;
    this.checkpointMark = checkpointMark;
    this.prevWatermarkTimestamp = prevWatermarkTimestamp;
    this.checkpointMarkCoder = checkpointMarkCoder;
    this.unboundedSource = unboundedSource;
    this.stateMap = stateMap;
    this.stateCoderMap = stateCoderMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
  }

  public ByteBuf encode() {
    final FSTConfiguration conf = FSTSingleton.getInstance();
    final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    final DataOutputStream dos = new DataOutputStream(bos);

    try {
      dos.writeInt(TASK_READY.ordinal());
      dos.writeUTF(taskId);
      conf.encodeToStream(bos, taskLocationMap);

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

      conf.encodeToStream(bos, taskExecutorIdMap);

      LOG.info("Encoding state done for {}, size: {}", taskId, byteBuf.readableBytes());

      dos.close();
      bos.close();
      return byteBuf;
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static ReadyTask decode(final InputStream inputStream) {

    final FSTConfiguration conf = FSTSingleton.getInstance();

    final DataInputStream dis = new DataInputStream(inputStream);
    try {
      final String taskId = dis.readUTF();
      final Map<String, TaskLoc> taskLocationMap =
        (Map<String, TaskLoc>) conf.decodeFromStream(inputStream);

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

      final Map<String, String> taskExecutorMap = (Map<String, String>) conf.decodeFromStream(inputStream);

      return new ReadyTask(taskId,
        taskLocationMap,
        checkpointMark,
        checkpointMarkCoder,
        prevWatermarkTimestamp,
        unboundedSource,
        stateMap,
        stateCoderMap,
        taskExecutorMap);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
