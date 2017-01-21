/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.vortex.client;

import edu.snu.vortex.runtime.driver.VortexDriver;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.EnvironmentUtils;

public final class JobLauncher {
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(JobLauncher.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "Vortex-Starlab")
        .set(DriverConfiguration.ON_DRIVER_STARTED, VortexDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, VortexDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, VortexDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_MESSAGE, VortexDriver.TaskMessageHandler.class)
        .build();
  }

  public static void main(final String[] args) throws Exception {

  }
}
