package edu.snu.coral.compiler.frontend.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.BlockManager;

public class SparkEnv extends org.apache.spark.SparkEnv {
  public SparkEnv(final SparkConf conf, final BlockManager blockManager) {
    super("ENV", null, null, null, null, null,
        null, null, blockManager, null, null,
        null, null, conf);
  }
}
