package org.apache.nemo.offloading.common;

public  class Constants {
  public static final int VM_WORKER_PORT = 25321;

  public static final int LAMBDA_WARMUP = 40; // sec

  //public static final String SIDEINPUT_LAMBDA_NAME2 = "nemo-dev-imhandler";
  public static final String SIDEINPUT_LAMBDA_NAME2 = "nemo-dev-tg-erverless-worker";

  public static final int FLUSH_BYTES = 10 * 1024 * 1024; // MB

  public static final boolean enableLambdaLogging = false;
}
