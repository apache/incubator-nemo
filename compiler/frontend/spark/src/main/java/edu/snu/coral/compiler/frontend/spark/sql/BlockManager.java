package edu.snu.coral.compiler.frontend.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.storage.BlockInfoManager;
import org.apache.spark.storage.BlockManagerMaster;

import java.io.Serializable;

public class BlockManager extends org.apache.spark.storage.BlockManager {
  public BlockManager(final SparkConf conf, final BlockManagerMaster master,
                      final MemoryManager memoryManager, final RpcEnv rpcEnv) {
    super(null, rpcEnv, master, null, conf, memoryManager,
        null, null, null, null, 4);
  }

  @Override
  public BlockInfoManager blockInfoManager() {
    return new BlockInfoManager();
  }
}
