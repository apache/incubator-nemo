package org.apache.nemo.runtime.master.lambda;

public interface LambdaContainerRequester {
  void createRequest(String address, int port, int requestId, String executorId);
}
