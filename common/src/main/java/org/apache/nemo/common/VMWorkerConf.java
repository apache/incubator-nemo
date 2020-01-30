package org.apache.nemo.common;

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class VMWorkerConf {

  public final int executorThreadNum;
  public final Map<String, Pair<String, Integer>> executorAddrMap;
  public final Map<String, byte[]> serializerMap;
  public final Map<String, String> taskExecutorIdMap;
  public final Map<TransferKey, Integer> taskTransferIndexMap;
  public final String rendevousServerAddr;
  public final int rendevousServerPort;
  public final String nameServerAddr;
  public final int nameServerPort;

  public VMWorkerConf(final int executorThreadNum,
                      final Map<String, Pair<String, Integer>> executorAddrMap,
                      final Map<String, byte[]> serializerMap,
                      final Map<String, String> taskExecutorIdMap,
                      final Map<TransferKey, Integer> taskTransferIndexMap,
                      final String rendevousServerAddr,
                      final int rendevousServerPort,
                      final String nameServerAddr,
                      final int nameServerPort) {
    this.executorThreadNum = executorThreadNum;
    this.executorAddrMap = executorAddrMap;
    this.serializerMap = serializerMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.rendevousServerAddr = rendevousServerAddr;
    this.rendevousServerPort = rendevousServerPort;
    this.nameServerAddr = nameServerAddr;
    this.nameServerPort = nameServerPort;
  }

  public ByteBuf encodeWithoutExecutorId() {
    final ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    try {
      bos.writeInt(executorThreadNum);
      SerializationUtils.serialize((ConcurrentHashMap) executorAddrMap, bos);
      SerializationUtils.serialize((ConcurrentHashMap) serializerMap, bos);
      SerializationUtils.serialize((ConcurrentHashMap) taskExecutorIdMap, bos);
      SerializationUtils.serialize((ConcurrentHashMap) taskTransferIndexMap, bos);

      bos.writeUTF(rendevousServerAddr);
      bos.writeInt(rendevousServerPort);
      bos.writeUTF(nameServerAddr);
      bos.writeInt(nameServerPort);

      bos.close();

      return byteBuf;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static VMWorkerConf decode(final ByteBuf byteBuf) {
    try {
      final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
      final int executorThreadNum = bis.readInt();
      final Map<String, Pair<String, Integer>> executorAddrMap = SerializationUtils.deserialize(bis);
      final Map<String, byte[]> serializerMap = SerializationUtils.deserialize(bis);
      final Map<String, String> taskExecutorIdMap = SerializationUtils.deserialize(bis);
      final Map<TransferKey, Integer> taskTransferIndexMap =
        SerializationUtils.deserialize(bis);

      final String rendevousServerAddr = bis.readUTF();
      final int rendevousServerPort = bis.readInt();

      final String nameServerAddr = bis.readUTF();
      final int nameServerPort = bis.readInt();

      return new VMWorkerConf(executorThreadNum,
        executorAddrMap,
        serializerMap,
        taskExecutorIdMap,
        taskTransferIndexMap,
        rendevousServerAddr,
        rendevousServerPort,
        nameServerAddr,
        nameServerPort);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
