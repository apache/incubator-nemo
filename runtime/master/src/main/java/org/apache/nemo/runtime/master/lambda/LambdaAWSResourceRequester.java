package org.apache.nemo.runtime.master.lambda;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.AWSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public final class LambdaAWSResourceRequester implements LambdaContainerRequester {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaAWSResourceRequester.class.getName());

  private final AWSLambdaAsync awsLambda;
  private static final String FUNCTION_NAME = "LAMBDA";

  private final AtomicInteger numLambdaCreated = new AtomicInteger(0);

  private final int maxLambda;
  private final Boolean[] lambdaCreated;

  @Inject
  private LambdaAWSResourceRequester(final EvalConf evalConf) {
    final ProfileCredentialsProvider provider = new ProfileCredentialsProvider(evalConf.awsProfileName);
    this.maxLambda = evalConf.numMaxLambda;
    this.lambdaCreated = new Boolean[maxLambda];

    for (int i = 0; i < maxLambda; i++) {
      lambdaCreated[i] = false;
    }

    this.awsLambda = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(evalConf.awsRegion)
      .withCredentials(provider)
      .withClientConfiguration(
        new ClientConfiguration().withMaxConnections(500)).build();

    // Registration
  }

  private int findNextFreeLambda() {
    if (numLambdaCreated.get() == maxLambda) {
      return -1;
    }

    for (int i = 0; i < maxLambda; i++) {
      if (!lambdaCreated[i]) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public LambdaActivator createRequest(String address, int port,
                                              final int requestId,
                                              final String executorId,
                                              String containerType,
                                              int capacity,
                                              int slot,
                                              int memory) {
    final int nextLambdaIdx = findNextFreeLambda();
    if (nextLambdaIdx < 0) {
      throw new RuntimeException("Cannot create new lambda ... curr num " + numLambdaCreated + ", max "
        + maxLambda + ", " + lambdaCreated);
    }

    final String lambdaName = AWSUtils.LAMBDA_EXECUTOR_PREFIX + (nextLambdaIdx + 1);
    final InvokeRequest request = new InvokeRequest()
      .withFunctionName(lambdaName)
      .withPayload(String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
        address, port, requestId));

    LOG.info("Invoke create lambda request for requestId: {}/{} lambdaName: {}/{}",
      requestId, executorId, lambdaName, request);
    final Future<InvokeResult> future = awsLambda.invokeAsync(request);

    return new LambdaActivator() {
      @Override
      public void activate() {
        final InvokeRequest request = new InvokeRequest()
          .withFunctionName(lambdaName)
          .withPayload(String.format("{\"address\":\"%s\", \"port\": %d, \"requestId\": %d}",
            address, port, requestId));

        LOG.info("Activate request for requestId: {}/{} lambdaName: {}/{}",
          requestId, executorId, lambdaName, request);
        final Future<InvokeResult> future = awsLambda.invokeAsync(request);
      }
    };
  }
}
