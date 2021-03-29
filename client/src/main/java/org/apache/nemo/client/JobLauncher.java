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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.nemo.common.PublicAddressProvider;
import org.apache.nemo.common.ResourceSpecBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.backend.nemo.NemoPlanRewriter;
import org.apache.nemo.conf.EvalConf.SamplingPath;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.driver.NemoDriver;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.master.lambda.LambdaAWSResourceRequester;
import org.apache.nemo.runtime.master.lambda.LambdaContainerRequester;
import org.apache.nemo.runtime.master.lambda.LambdaYarnResourceRequester;
import org.apache.nemo.runtime.master.offloading.LambdaOffloadingRequester;
import org.apache.nemo.runtime.master.offloading.OffloadingRequester;
import org.apache.nemo.runtime.master.offloading.YarnExecutorOffloadingRequester;
import org.apache.nemo.runtime.master.scheduler.Scheduler;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageParameters;
import org.apache.nemo.runtime.message.netty.NettyMasterEnvironment;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.parameters.JobMessageHandler;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Job launcher.
 */
public final class JobLauncher {

  static {
    System.out.println(
        "\nPowered by\n"
          + "    _   __                   \n"
          + "   / | / /__  ____ ___  ____ \n"
          + "  /  |/ / _ \\/ __ `__ \\/ __ \\\n"
          + " / /|  /  __/ / / / / / /_/ /\n"
          + "/_/ |_/\\___/_/ /_/ /_/\\____/ \n"
    );
  }

  private static final Tang TANG = Tang.Factory.getTang();
  private static final Logger LOG = LoggerFactory.getLogger(JobLauncher.class.getName());
  private static final int LOCAL_NUMBER_OF_EVALUATORS = 100; // hopefully large enough for our use....
  private static Configuration jobAndDriverConf = null;
  private static Configuration deployModeConf = null;
  private static Configuration builtJobConf = null;

  private static DriverLauncher driverLauncher;
  private static DriverRPCServer driverRPCServer;

  private static CountDownLatch driverReadyLatch;
  private static CountDownLatch driverShutdownedLatch;
  private static CountDownLatch jobDoneLatch;
  private static String serializedDAG;
  private static final List<?> COLLECTED_DATA = new ArrayList<>();
  private static final String[] EMPTY_USER_ARGS = new String[0];

  /**
   * private constructor.
   */
  private JobLauncher() {
  }

  private static void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        shutdown(false);
      }
    });
  }

  /**
   * Main JobLauncher method.
   *
   * @param args arguments.
   * @throws Exception exception on the way.
   */
  public static void main(final String[] args) throws Exception {
    try {
      registerShutdownHook();
      setup(args);
      // Launch client main. The shutdown() method is called inside the launchDAG() method.
      runUserProgramMain(builtJobConf);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Set up the driver, etc. before the actual execution.
   * @param args arguments.
   * @throws InjectionException injection exception from REEF.
   * @throws ClassNotFoundException class not found exception.
   * @throws IOException IO exception.
   */
  public static void setup(final String[] args) throws InjectionException, ClassNotFoundException, IOException {
    // Get Job and Driver Confs
    builtJobConf = getJobConf(args);

    final Injector inj = Tang.Factory.getTang().newInjector(builtJobConf);
    final String samplingJsonPath = inj.getNamedInstance(SamplingPath.class);

    if (!samplingJsonPath.isEmpty()) {
      final String content = new String(Files.readAllBytes(Paths.get(samplingJsonPath)), StandardCharsets.UTF_8);
      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      jcb.bindNamedParameter(EvalConf.SamplingJsonString.class, content);
      builtJobConf = Configurations.merge(builtJobConf, jcb.build());
    }


    // Registers actions for launching the DAG.
    LOG.info("Launching RPC Server");
    driverRPCServer = new DriverRPCServer();
    driverRPCServer
      .registerHandler(ControlMessage.DriverToClientMessageType.DriverStarted, event -> {
      })
      .registerHandler(ControlMessage.DriverToClientMessageType.KillAll, event -> {
        try {
          LOG.info("kill all....");
          Process p = Runtime.getRuntime().exec("touch signal_kill.txt");
          p.wait();
        } catch (Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      })
      .registerHandler(ControlMessage.DriverToClientMessageType.DriverReady, event -> driverReadyLatch.countDown())
      .registerHandler(ControlMessage.DriverToClientMessageType.ExecutionDone, event -> jobDoneLatch.countDown())
      .registerHandler(ControlMessage.DriverToClientMessageType.DataCollected, message -> COLLECTED_DATA.addAll(
        SerializationUtils.deserialize(Base64.getDecoder().decode(message.getDataCollected().getData()))))
      .registerHandler(ControlMessage.DriverToClientMessageType.DriverShutdowned, event -> {
        LOG.info("Driver shutdown message received !");
        if (!shutdowned) {
          LOG.info("Driver shutdown message received ! shutdown latch");
          shutdown(driverShutdownedLatch == null);
        } else {
          driverShutdownedLatch.countDown();
        }
      })
      .run();

    final Configuration driverConf = getDriverConf(builtJobConf);
    final Configuration driverNcsConf = getDriverNcsConf();
    final Configuration driverMessageConfig = getDriverMessageConf();
    final String defaultExecutorResourceConfig = "[{\"type\":\"Transient\",\"memory_mb\":512,\"capacity\":5},"
      + "{\"type\":\"Reserved\",\"memory_mb\":512,\"capacity\":5}]";

    final Injector confInjector = TANG.newInjector(builtJobConf);

    final String resources = ResourceSpecBuilder.builder().addResource(
      ResourceSpecBuilder.ResourceType.Reserved,
      confInjector.getNamedInstance(JobConf.ExecutorMem.class),
      confInjector.getNamedInstance(JobConf.ExecutorYarnCore.class),
      confInjector.getNamedInstance(EvalConf.TaskSlot.class),
      confInjector.getNamedInstance(JobConf.NumExecutor.class)).build();

    // final Configuration executorResourceConfig = Tang.Factory.getTang().newConfigurationBuilder()
    //  .bindNamedParameter(JobConf.ExecutorJSONContents.class, resources)
    //  .build();

    final Configuration executorResourceConfig = getJSONConf(builtJobConf, JobConf.ExecutorJSONPath.class,
     JobConf.ExecutorJSONContents.class, defaultExecutorResourceConfig);

    final Configuration bandwidthConfig = getJSONConf(builtJobConf, JobConf.BandwidthJSONPath.class,
      JobConf.BandwidthJSONContents.class, "");
    final Configuration clientConf = getClientConf();
    final Configuration schedulerConf = getSchedulerConf(builtJobConf);

    // Merge Job and Driver Confs
    jobAndDriverConf = Configurations.merge(builtJobConf, driverConf, driverNcsConf, driverMessageConfig,
      executorResourceConfig, bandwidthConfig, driverRPCServer.getListeningConfiguration(), schedulerConf);

    // Get DeployMode Conf
    deployModeConf = Configurations.merge(getDeployModeConf(builtJobConf), clientConf);

    // Start Driver and launch user program.
    if (jobAndDriverConf == null || deployModeConf == null || builtJobConf == null) {
      throw new RuntimeException("Configuration for launching driver is not ready");
    }

    // Launch driver
    LOG.info("Launching driver");
    driverReadyLatch = new CountDownLatch(1);
    driverLauncher = DriverLauncher.getLauncher(deployModeConf);
    driverLauncher.submit(jobAndDriverConf, 500);
    // When the driver is up and the resource is ready, the DriverReady message is delivered.
  }

  /**
   * Clean up everything.
   */
  private static boolean shutdowned = false;
  public static synchronized void shutdown(boolean withoutLatch) {
    if (!shutdowned) {
      // Trigger driver shutdown afterwards

      shutdowned = true;

      if (!withoutLatch) {
        driverShutdownedLatch = new CountDownLatch(1);
      }
      scalingService.shutdownNow();
      scalingSchedService.shutdownNow();
      LOG.info("Scaling service shutdown now");

      if (driverRPCServer.hasLink()) {
        driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
          .setType(ControlMessage.ClientToDriverMessageType.DriverShutdown).build());
        // Wait for driver to naturally finish

        if (!withoutLatch) {
          synchronized (driverLauncher) {
            // while (!driverLauncher.getStatus().isDone()) {
            try {
              LOG.info("Driver shutdown latch await");
              driverShutdownedLatch.await();
              // LOG.info("Wait for the driver to finish");
              // driverLauncher.wait();
            } catch (final InterruptedException e) {
              LOG.warn("Interrupted: ", e);
              // clean up state...
              Thread.currentThread().interrupt();
            }
            // }
            LOG.info("Driver terminated");
          }
        }
      }

      // Close everything that's left
      driverRPCServer.shutdown();
      driverLauncher.close();
      final Optional<Throwable> possibleError = driverLauncher.getStatus().getError();
      if (possibleError.isPresent()) {
        throw new RuntimeException(possibleError.get());
      } else {
        LOG.info("Job successfully completed");
      }

    }
  }

  /**
   * Launch application using the application DAG.
   * Notice that we launch the DAG one at a time, as the result of a DAG has to be immediately returned to the
   * Java variable before the application can be resumed.
   *
   * @param dag the application DAG.
   */
  // When modifying the signature of this method, see CompilerTestUtil#compileDAG and make corresponding changes
  public static void launchDAG(final IRDAG dag) {
    launchDAG(dag, Collections.emptyMap(), "");
  }

  /**
   * @param dag the application DAG.
   * @param jobId job ID.
   */
  public static void launchDAG(final IRDAG dag, final String jobId) {
    launchDAG(dag, Collections.emptyMap(), jobId);
  }

  private static final ExecutorService scalingService = Executors.newSingleThreadExecutor();
  private static final ScheduledExecutorService scalingSchedService = Executors.newSingleThreadScheduledExecutor();

  /**
   * @param dag the application DAG.
   * @param broadcastVariables broadcast variables (can be empty).
   * @param jobId job ID.
   */
  public static void launchDAG(final IRDAG dag,
                               final Map<Serializable, Object> broadcastVariables,
                               final String jobId) {
    // launch driver if it hasn't been already
    if (driverReadyLatch == null) {
      try {
        setup(new String[]{"-job_id", jobId});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // Wait until the driver is ready.
    try {
      LOG.info("Waiting for the driver to be ready");
      driverReadyLatch.await();
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted: " + e);
      // clean up state...
      Thread.currentThread().interrupt();
    }

    LOG.info("Launching DAG...");
    serializedDAG = Base64.getEncoder().encodeToString(SerializationUtils.serialize(dag));
    jobDoneLatch = new CountDownLatch(1);
    driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
        .setType(ControlMessage.ClientToDriverMessageType.LaunchDAG)
        .setLaunchDAG(ControlMessage.LaunchDAGMessage.newBuilder()
            .setDag(serializedDAG)
            .setBroadcastVars(ByteString.copyFrom(SerializationUtils.serialize((Serializable) broadcastVariables)))
            .build())
        .build());

    final String home = System.getenv("HOME");

    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(home + "/incubator-nemo/scaling.txt"));
      writer.close();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    scalingService.execute(() -> {
      LOG.info("Scaling service invoked...");
        try {
          final BufferedReader br =
            new BufferedReader(new FileReader(home + "/incubator-nemo/scaling.txt"));

          String s;
          String lastLine = null;
          int cnt = 0;

          // skip first
          while ((s = br.readLine()) != null) {
            lastLine = s;
            cnt += 1;
          }

          while (!shutdowned) {

            while ((s = br.readLine()) == null) {
              Thread.sleep(10);
            }

            lastLine = s;

            LOG.info("Read line {} ... send Scaling", lastLine);

            String[] split = lastLine.split(" ");
            final String decision = split[0];

            if (decision.equals("oratio")) {
              final int numStages = split.length - 2;
              final double offloadDivide = Double.valueOf(split[1]);
              final List<Double> stageRatio = new ArrayList<>(numStages);

              for (int i = 2; i < split.length; i++) {
                stageRatio.add(Double.valueOf(split[i]));
              }

              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision(decision)
                  .setDivide(offloadDivide)
                  .addAllStageRatio(stageRatio)
                  .build())
                .build());

            } else if (decision.equals("o") || decision.equals("no") || decision.equals("op")) {
              final double offloadDivide = Double.valueOf(split[1]);

              if (split.length == 3) {
                final int queryNum = Integer.valueOf(split[2]);
                driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                  .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                  .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                    .setDecision(decision)
                    .setDivide(offloadDivide)
                    .setQuery(queryNum)
                    .build())
                  .build());
              } else {
                // length 2
                driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                  .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                  .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                    .setDecision(decision)
                    .setDivide(offloadDivide)
                    .build())
                  .build());
              }
            } else if (decision.equals("i")) {
              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision(decision)
                  .build())
                .build());
            } else if (decision.equals("pa")) {
              // Proactive migration
              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision(decision)
                  .build())
                .build());
            } else if (decision.equals("info")) {
              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision(decision)
                  .setInfo(lastLine)
                  .build())
                .build());

            } else if (decision.equals("add-yarn") || decision.equals("add-offloading-executor")) {
              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision(decision)
                  .setInfo(lastLine)
                  .build())
                .build());

            } else {
              LOG.info("Send " + lastLine);
              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision(decision)
                  .setInfo(lastLine)
                  .build())
                .build());
            }
          }
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
    });

    // input rate 보내기
    try {
      final BufferedReader br =
        new BufferedReader(new FileReader(home + "/incubator-nemo/source.log"));

      Pattern pattern = Pattern.compile("\\d+ events");

      final AtomicBoolean prevNull = new AtomicBoolean(false);

      scalingSchedService.scheduleAtFixedRate(() -> {

        try {
          String line;
          boolean sent = false;
          while ((line = br.readLine()) != null) {
            sent = true;
            final Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
              final String inputRateStr = matcher.group();
              LOG.info("Input rate {}", inputRateStr);
              final String[] s = inputRateStr.split(" events");
              final String inputRateCmd = "INPUT " + s[0];
              driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
                .setType(ControlMessage.ClientToDriverMessageType.Scaling)
                .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                  .setDecision("info")
                  .setInfo(inputRateCmd)
                  .build())
                .build());
            }
          }

          if (!sent && prevNull.get()) {
            driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
              .setType(ControlMessage.ClientToDriverMessageType.Scaling)
              .setScalingMsg(ControlMessage.ScalingMessage.newBuilder()
                .setDecision("info")
                .setInfo("INPUT 0")
                .build())
              .build());
          } else if (!sent) {
            prevNull.set(true);
          } else {
            prevNull.set(false);
          }

        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }


      }, 1, 1, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Wait for the ExecutionDone message from the driver
    try {
      LOG.info("Waiting for the DAG to finish execution");
      jobDoneLatch.await();
    } catch (final InterruptedException e) {
      LOG.warn("Interrupted: " + e);
      // clean up state...
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      LOG.info("DAG execution done");
      // trigger shutdown.
      shutdown(false);
    }
  }

  /**
   * Run user-provided main method.
   *
   * @param jobConf the job configuration
   * @throws Exception on any exceptions on the way
   */
  private static void runUserProgramMain(final Configuration jobConf) throws Exception {
    final Injector injector = TANG.newInjector(jobConf);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);

    final String userArgsString = injector.getNamedInstance(JobConf.UserMainArguments.class);
    final String[] args = userArgsString.isEmpty() ? EMPTY_USER_ARGS : userArgsString.split(" ");
    LOG.info("Args are {}", Arrays.toString(args));
    final Class userCode = Class.forName(className);
    LOG.info("User code {}", userCode);
    final Method method = userCode.getMethod("main", String[].class);
    LOG.info("Method {}", method);
    if (!Modifier.isStatic(method.getModifiers())) {
      LOG.info("NO1 {}", method);
      throw new RuntimeException("User Main Method not static");
    }
    if (!Modifier.isPublic(userCode.getModifiers())) {
      LOG.info("NO2 {}", method);
      throw new RuntimeException("User Main Class not public");
    }

    LOG.info("User program started");
    try {
      method.invoke(null, (Object) args);
    } catch (Throwable t) {
      LOG.info("Bad start");
      t.printStackTrace();
      StringWriter errors = new StringWriter();
      t.printStackTrace(new PrintWriter(errors));
      LOG.info(errors.toString());
      throw new RuntimeException("Bad");
    }
    LOG.info("User program finished");
  }

  /**
   * @return client configuration.
   */
  private static Configuration getClientConf() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(JobMessageHandler.class, NemoClient.JobMessageHandler.class);
    return jcb.build();
  }

  /**
   * Fetch scheduler configuration.
   * @param jobConf job configuration.
   * @return the scheduler configuration.
   * @throws ClassNotFoundException exception while finding the class.
   * @throws InjectionException exception while injection (REEF Tang).
   */
  private static Configuration getSchedulerConf(final Configuration jobConf)
    throws ClassNotFoundException, InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String classImplName = injector.getNamedInstance(JobConf.SchedulerImplClassName.class);
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Class schedulerImpl = ((Class<Scheduler>) Class.forName(classImplName));
    jcb.bindImplementation(Scheduler.class, schedulerImpl);
    jcb.bindImplementation(PlanRewriter.class, NemoPlanRewriter.class);
    return jcb.build();
  }

  /**
   * Get driver ncs configuration.
   *
   * @return driver ncs configuration.
   * @throws InjectionException exception while injection.
   */
  private static Configuration getDriverNcsConf() throws InjectionException {
    return Configurations.merge(NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        TANG.newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  /**
   * Get driver message configuration.
   *
   * @return driver message configuration.
   * @throws InjectionException exception while injection.
   */
  private static Configuration getDriverMessageConf() throws InjectionException {
    return TANG.newConfigurationBuilder()
        .bindNamedParameter(MessageParameters.SenderId.class,
          MessageEnvironment.MASTER_ID)
        .build();
  }

  /**
   * Get driver configuration.
   *
   * @param jobConf job Configuration to get job id and driver memory.
   * @return driver configuration.
   * @throws InjectionException exception while injection.
   */
  private static Configuration getDriverConf(final Configuration jobConf) throws InjectionException, ClassNotFoundException {
    final Injector injector = TANG.newInjector(jobConf);
    final String jobId = injector.getNamedInstance(JobConf.JobId.class);
    final int driverMemory = injector.getNamedInstance(JobConf.DriverMemMb.class);

    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);

    final String excludeJars = injector.getNamedInstance(JobConf.ExcludeJars.class);
    final List<String> excludeJarList;

    if (!excludeJars.isEmpty()) {
      excludeJarList = Arrays.asList(excludeJars.split(":"));
    } else {
      excludeJarList = Collections.emptyList();
    }


    return DriverConfiguration.CONF
        .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getAllClasspathJars()
        .stream().filter(path -> {
          LOG.info("Library path: {}", path);
          for (final String excludeJar : excludeJarList) {
            if (path.contains(excludeJar)) {
              LOG.info("Excplude path: {}", path);
              return false;
            }
          }
          return true;
          }).collect(Collectors.toSet()))
        .set(DriverConfiguration.ON_DRIVER_STARTED, NemoDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, NemoDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, NemoDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, NemoDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, NemoDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, NemoDriver.DriverStopHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId)
        .set(DriverConfiguration.DRIVER_MEMORY, driverMemory)
        .build();
  }

  private static Class<? extends OffloadingRequester> getRequesterConf(final String offloadingType) {

    if (offloadingType.equals("lambda")) {
      return LambdaOffloadingRequester.class;
    } else {
      return YarnExecutorOffloadingRequester.class;
    }
  }

  private static Class<? extends LambdaContainerRequester>
  getLambdaRequesterConf(final String offloadingType) {
    if (offloadingType.equals("lambda")) {
      return LambdaAWSResourceRequester.class;
    } else {
      return LambdaYarnResourceRequester.class;
    }
  }

  /**
   * Get job configuration.
   *
   * @param args arguments to be processed as command line.
   * @return job configuration.
   * @throws IOException        exception while processing command line.
   * @throws InjectionException exception while injection.
   */
  @VisibleForTesting
  public static Configuration getJobConf(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder confBuilder = TANG.newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(JobConf.JobId.class);
    cl.registerShortNameOfClass(JobConf.ExcludeJars.class);
    cl.registerShortNameOfClass(JobConf.UserMainClass.class);
    cl.registerShortNameOfClass(JobConf.UserMainArguments.class);
    cl.registerShortNameOfClass(JobConf.DAGDirectory.class);
    cl.registerShortNameOfClass(JobConf.OptimizationPolicy.class);
    cl.registerShortNameOfClass(JobConf.DeployMode.class);
    cl.registerShortNameOfClass(JobConf.DriverMemMb.class);
    cl.registerShortNameOfClass(JobConf.ExecutorJSONPath.class);
    cl.registerShortNameOfClass(JobConf.BandwidthJSONPath.class);
    cl.registerShortNameOfClass(JobConf.JVMHeapSlack.class);
    cl.registerShortNameOfClass(JobConf.IORequestHandleThreadsTotal.class);
    cl.registerShortNameOfClass(JobConf.MaxTaskAttempt.class);
    cl.registerShortNameOfClass(JobConf.FileDirectory.class);
    cl.registerShortNameOfClass(JobConf.GlusterVolumeDirectory.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerPort.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerBacklog.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerNumListeningThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerNumWorkingThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportClientNumThreads.class);
    cl.registerShortNameOfClass(JobConf.MaxNumDownloadsForARuntimeEdge.class);
    cl.registerShortNameOfClass(JobConf.SchedulerImplClassName.class);
    cl.registerShortNameOfClass(JobConf.ExecutorMem.class);
    cl.registerShortNameOfClass(JobConf.ExecutorYarnCore.class);
    cl.registerShortNameOfClass(JobConf.NumExecutor.class);

    EvalConf.registerCommandLineArgument(cl);

    cl.processCommandLine(args);

    final Configuration conf = confBuilder.build();
    final String offloadingType = Tang.Factory.getTang().newInjector(conf).getNamedInstance(EvalConf.OffloadingType.class);
    final Configuration c = Tang.Factory.getTang().newConfigurationBuilder()
      .bindImplementation(OffloadingRequester.class, getRequesterConf(offloadingType))
      .bindImplementation(LambdaContainerRequester.class, getLambdaRequesterConf(offloadingType))
      .bindImplementation(MessageEnvironment.class, NettyMasterEnvironment.class)
      .bindImplementation(LocalAddressProvider.class, PublicAddressProvider.class)
      .build();

    return Configurations.merge(conf, c);
  }

  /**
   * Get deploy mode configuration.
   *
   * @param jobConf job configuration to get deploy mode.
   * @return deploy mode configuration.
   * @throws InjectionException exception while injection.
   */
  private static Configuration getDeployModeConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String deployMode = injector.getNamedInstance(JobConf.DeployMode.class);
    switch (deployMode) {
      case "local":
        return LocalRuntimeConfiguration.CONF
            .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, LOCAL_NUMBER_OF_EVALUATORS)
            .build();
      case "yarn":
        return YarnClientConfiguration.CONF
            .set(YarnClientConfiguration.JVM_HEAP_SLACK, injector.getNamedInstance(JobConf.JVMHeapSlack.class))
            .build();
      default:
        throw new UnsupportedOperationException(deployMode);
    }
  }

  /**
   * Read json file and return its contents as configuration parameter.
   *
   * @param jobConf           job configuration to get json path.
   * @param pathParameter     named parameter represents path to the json file, or an empty string
   * @param contentsParameter named parameter represents contents of the file
   * @param defaultContent    the default configuration
   * @return configuration with contents of the file, or an empty string as value for {@code contentsParameter}
   * @throws InjectionException exception while injection.
   */
  private static Configuration getJSONConf(final Configuration jobConf,
                                           final Class<? extends Name<String>> pathParameter,
                                           final Class<? extends Name<String>> contentsParameter,
                                           final String defaultContent)
      throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    try {
      final String path = injector.getNamedInstance(pathParameter);
      final String contents = path.isEmpty() ? defaultContent
        : new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
      return TANG.newConfigurationBuilder()
          .bindNamedParameter(contentsParameter, contents)
          .build();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the built job configuration.
   * It can be {@code null} if this method is not called by the process which called the main function of this class.
   *
   * @return the built job configuration.
   */
  public static Configuration getBuiltJobConf() {
    return builtJobConf;
  }

  /**
   * Get the collected data.
   *
   * @param <T> the type of the data.
   * @return the collected data.
   */
  public static <T> List<T> getCollectedData() {
    final List<T> result = (List<T>) new ArrayList<>(COLLECTED_DATA);
    COLLECTED_DATA.clear(); // flush after fetching.
    return result;
  }
}
