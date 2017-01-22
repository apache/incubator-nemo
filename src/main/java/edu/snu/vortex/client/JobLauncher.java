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

import edu.snu.vortex.runtime.driver.Parameters;
import edu.snu.vortex.runtime.driver.VortexDriver;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class JobLauncher {

  private static final Logger LOG = Logger.getLogger(JobLauncher.class.getName());

  private static final int MAX_NUMBER_OF_EVALUATORS = 1000;

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

  private static Configuration getRuntimeConfiguration(final String runtime) {
    if (runtime.equals("yarn")) {
      return getYarnRuntimeConfiguration();
    } else {
      return getLocalRuntimeConfiguration();
    }
  }

  private static Configuration getYarnRuntimeConfiguration() {
    return YarnClientConfiguration.CONF.build();
  }

  private static Configuration getLocalRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
  }

  private static Configuration getJobConf(final String[] args) throws Exception {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Parameters.Runtime.class);
    cl.registerShortNameOfClass(Parameters.EvaluatorCore.class);
    cl.registerShortNameOfClass(Parameters.EvaluatorMem.class);
    cl.registerShortNameOfClass(Parameters.EvaluatorNum.class);
    cl.registerShortNameOfClass(Parameters.UserArguments.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  public static void main(final String[] args) throws Exception {
    final Configuration jobConf = getJobConf(args);

    final String runtime = Tang.Factory.getTang().newInjector(jobConf)
        .getNamedInstance(Parameters.Runtime.class);

    final Configuration runtimeConf = getRuntimeConfiguration(runtime);
    final Configuration driverConf = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(Configurations.merge(driverConf, jobConf));
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
