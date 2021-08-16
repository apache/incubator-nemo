/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nemo.client;

import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Utility class for the Client.
 */
public final class ClientUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class.getName());

  /**
   * Private constructor.
   */
  private ClientUtils() {
  }

  /**
   * Handler for the launch optimization message.
   *
   * @param message the message received from the driver.
   * @param driverRPCServer driver RPC server.
   */
  static void handleOptimizationType(final ControlMessage.DriverToClientMessage message,
                                     final DriverRPCServer driverRPCServer) {
    if (message.getOptimizationType().equals(ControlMessage.OptimizationType.XGBoost)) {
      new Thread(() ->
        driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
          .setType(ControlMessage.ClientToDriverMessageType.Notification)
          .setMessage(ControlMessage.NotificationMessage.newBuilder()
            .setOptimizationType(ControlMessage.OptimizationType.XGBoost)
            .setData(ClientUtils.launchXGBoostScript(message.getDataCollected().getData()))
            .build())
          .build()))
        .start();
    }
  }

  /**
   * launches the XGBoost Script.
   *
   * @param irDagSummary the IR DAG to run the script for.
   * @return the results file converted into string.
   */
  private static String launchXGBoostScript(final String irDagSummary) {
    try {
      final String projectRootPath = Util.fetchProjectRootPath();
      final String scriptPath = projectRootPath + "/bin/xgboost_optimization.sh";
      // It trains the model with the metric data of previous jobs with the same IRDAG signature.
      final String[] command = {scriptPath, irDagSummary};
      LOG.info("Running the python script at {}", scriptPath);
      final ProcessBuilder builder = new ProcessBuilder(command);
      builder.directory(new File(projectRootPath));
      builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
      builder.redirectError(ProcessBuilder.Redirect.INHERIT);
      final Process process = builder.start();
      process.waitFor();
      LOG.info("Python script execution complete!");

      final String resultsFile = projectRootPath + "/ml/results.out";
      LOG.info("Reading the results of the script at {}", resultsFile);
      final StringBuilder contentBuilder = new StringBuilder();
      try (Stream<String> stream = Files.lines(Paths.get(resultsFile), StandardCharsets.UTF_8)) {
        stream.forEach(s -> contentBuilder.append(s).append("\n"));
      }
      return contentBuilder.toString();
    } catch (IOException | InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MetricException(e);
    }
  }
}
