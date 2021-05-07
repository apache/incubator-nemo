package org.apache.nemo.runtime.master.lambda;

public interface LambdaContainerRequester {
  LambdaActivator createRequest(String address, int port, String containerType, int capacity, int slot, int memory);

  public interface LambdaActivator {
    void activate();
    int getRequestId();
    String getExecutorId();
  }
}
