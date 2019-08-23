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
package org.apache.nemo.conf;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Data plane Configuration for Executors.
 */
public final class DataPlaneConf {

  private final int numIOThreads;
  private final int maxNumDownloads;
  private final int scheduleSerThread;
  private final int serverPort;
  private final int clientNumThreads;
  private final int serverBackLog;
  private final int listenThreads;
  private final int workThreads;
  private final int maxOffHeapMb;
  private final int chunkSizeKb;

  @Inject
  private DataPlaneConf(@Parameter(JobConf.IORequestHandleThreadsTotal.class) final int numIOThreads,
                        @Parameter(JobConf.MaxNumDownloadsForARuntimeEdge.class) final int maxNumDownloads,
                        @Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                        @Parameter(JobConf.PartitionTransportServerPort.class) final int serverPort,
                        @Parameter(JobConf.PartitionTransportClientNumThreads.class) final int clientNumThreads,
                        @Parameter(JobConf.PartitionTransportServerBacklog.class) final int serverBackLog,
                        @Parameter(JobConf.PartitionTransportServerNumListeningThreads.class) final int listenThreads,
                        @Parameter(JobConf.PartitionTransportServerNumWorkingThreads.class) final int workThreads,
                        @Parameter(JobConf.MaxOffheapMb.class) final int maxOffHeapMb,
                        @Parameter(JobConf.ChunkSizeKb.class) final int chunkSizeKb) {
    this.numIOThreads = numIOThreads;
    this.maxNumDownloads = maxNumDownloads;
    this.scheduleSerThread = scheduleSerThread;
    this.serverPort = serverPort;
    this.clientNumThreads = clientNumThreads;
    this.serverBackLog = serverBackLog;
    this.listenThreads = listenThreads;
    this.workThreads = workThreads;
    this.maxOffHeapMb = maxOffHeapMb;
    this.chunkSizeKb = chunkSizeKb;
  }

  public Configuration getDataPlaneConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
      .bindNamedParameter(JobConf.IORequestHandleThreadsTotal.class, Integer.toString(numIOThreads))
      .bindNamedParameter(JobConf.MaxNumDownloadsForARuntimeEdge.class, Integer.toString(maxNumDownloads))
      .bindNamedParameter(JobConf.ScheduleSerThread.class, Integer.toString(scheduleSerThread))
      .bindNamedParameter(JobConf.PartitionTransportServerPort.class, Integer.toString(serverPort))
      .bindNamedParameter(JobConf.PartitionTransportClientNumThreads.class, Integer.toString(clientNumThreads))
      .bindNamedParameter(JobConf.PartitionTransportServerBacklog.class, Integer.toString(serverBackLog))
      .bindNamedParameter(JobConf.PartitionTransportServerNumListeningThreads.class, Integer.toString(listenThreads))
      .bindNamedParameter(JobConf.PartitionTransportServerNumWorkingThreads.class, Integer.toString(workThreads))
      .bindNamedParameter(JobConf.MaxOffheapMb.class, Integer.toString(maxOffHeapMb))
      .bindNamedParameter(JobConf.ChunkSizeKb.class, Integer.toString(chunkSizeKb))
      .build();
  }
 }
