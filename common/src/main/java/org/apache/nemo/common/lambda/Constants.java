package org.apache.nemo.common.lambda;

public  class Constants {
  public static final int VM_WORKER_PORT = 25321;
  public static final long MAIN_INPUT_PARTITION_SIZE = 10000;

  public static final int LAMBDA_WARMUP = 90; // sec
  public static final int POOL_SIZE = 140;

  public static final String SIDEINPUT_LAMBDA_NAME2 = "nemo-dev-imhandler";
}
