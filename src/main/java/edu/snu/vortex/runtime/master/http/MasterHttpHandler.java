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
package edu.snu.vortex.runtime.master.http;

import edu.snu.vortex.runtime.exception.ExecutorNotFoundException;
import edu.snu.vortex.runtime.exception.IrDagJsonNotFoundException;
import edu.snu.vortex.runtime.exception.StageNotFoundException;
import edu.snu.vortex.runtime.exception.TaskGroupNotFoundException;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.runtime.master.UserApplicationRunner;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.webserver.HttpHandler;
import org.apache.reef.webserver.ParsedHttpRequest;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handles HTTP requests sent to the Vortex Master.
 */
public final class MasterHttpHandler implements HttpHandler {
  private static final String KEY_IR_DAG = "ir-dag-key";
  private static final String KEY_EXECUTOR_ID = "executor-id";
  private static final String KEY_STAGE_ID = "stage-id";
  private static final String KEY_TASK_GROUP_ID = "task-group-id";

  private String uriSpecification = "vortex";
  private final InjectionFuture<RuntimeMaster> runtimeMaster;
  private final InjectionFuture<UserApplicationRunner> userApplicationRunner;

  @Inject
  private MasterHttpHandler(final InjectionFuture<RuntimeMaster> runtimeMaster,
                            final InjectionFuture<UserApplicationRunner> userApplicationRunner) {
    this.runtimeMaster = runtimeMaster;
    this.userApplicationRunner = userApplicationRunner;
  }

  @Override
  public String getUriSpecification() {
    return uriSpecification;
  }

  @Override
  public void setUriSpecification(final String s) {
    uriSpecification = s;
  }

  @Override
  public void onHttpRequest(final ParsedHttpRequest request, final HttpServletResponse response)
      throws IOException, ServletException {
    final String target = request.getTargetEntity().toLowerCase();
    final Map<String, List<String>> queryMap = request.getQueryMap();
    System.out.println("QueryMap" + queryMap.keySet().toString() + queryMap.values().toString());

    final Response result;
    switch (target) {
    case "ir-dag-keys":
      result = onIRDAGKeys();
      break;
    case "ir-dag-json":
      if (queryMap.isEmpty()) {
        result = Response.badRequest(String.format("The POST request should specify %s.", KEY_IR_DAG));
      } else {
        result = onIRDAGJson(queryMap);
      }
      break;
    case "job-dag":
      result = onJobDAG();
      break;
    case "executors":
      result = onExecutors();
      break;
    case "task-groups":
      if (queryMap.isEmpty()) {
        result = Response.badRequest(String.format("The POST request should specify %s.", KEY_EXECUTOR_ID));
      } else {
        result = onTaskGroups(queryMap);
      }
      break;
    case "task-group-info":
      if (queryMap.isEmpty()) {
        result = Response.badRequest(String.format("The POST request should specify %s.", KEY_TASK_GROUP_ID));
      } else {
        result = onTaskGroupInfo(queryMap);
      }
      break;
    case "task-group-list":
      if (queryMap.isEmpty()) {
        result = Response.badRequest(String.format("The POST request should specify %s.", KEY_STAGE_ID));
      } else {
        result = taskGroupList(queryMap);
      }
      break;
    case "stage-info":
      if (queryMap.isEmpty()) {
        result = Response.badRequest(String.format("The POST request should specify %s.", KEY_STAGE_ID));
      } else {
        result = onStageInfo(queryMap);
      }
      break;

    case "job-info":
      result = onJobInfo();
      break;
    default:
      result = Response.badRequest("Not implemented yet");
    }

    // Send response to the http client
    final int status = result.getStatus();
    final String message = result.getMessage();

    if (result.isOK()) {
      response.getOutputStream().println(message);
    } else {
      response.sendError(status, message);
    }
  }

  private Response onStageInfo(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get(KEY_STAGE_ID);
    if (args.size() != 1) {
      return Response.badRequest(String.format("Usage : only one %s at a time", KEY_STAGE_ID));
    }

    final String stageId = args.get(0);
    try {
      return Response.ok(runtimeMaster.get().getStageInfo(stageId));
    } catch (final StageNotFoundException e) {
      return Response.notFound(stageId);
    } catch (IOException e) {
      return Response.badRequest(e.getMessage());
    }
  }

  private Response onIRDAGKeys() {
    return Response.ok(userApplicationRunner.get().getIRDagKeys().toString());
  }

  private Response onIRDAGJson(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get(KEY_IR_DAG);
    if (args.size() != 1) {
      return Response.badRequest(String.format("Usage : only one %s at a time", KEY_IR_DAG));
    }

    final String irDagKey = args.get(0);
    try {
      return Response.ok(userApplicationRunner.get().getIRDagJsonByKey(irDagKey));
    } catch (final IrDagJsonNotFoundException e) {
      return Response.notFound(irDagKey);
    }
  }

  private Response onJobDAG() {
    return Response.ok(runtimeMaster.get().getJobDag());
  }

  private Response onExecutors() {
    return Response.ok(runtimeMaster.get().getExecutorsState());
  }

  private Response onTaskGroups(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get(KEY_EXECUTOR_ID);
    if (args.size() != 1) {
      return Response.badRequest(String.format("Usage : only one %s at a time", KEY_EXECUTOR_ID));
    }

    final String executorId = args.get(0);
    try {
      return Response.ok(runtimeMaster.get().getTaskGroups(executorId));
    } catch (final ExecutorNotFoundException e) {
      return Response.notFound(executorId);
    }
  }

  private Response onTaskGroupInfo(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get(KEY_TASK_GROUP_ID);
    if (args.size() != 1) {
      return Response.badRequest(String.format("Usage : only one %s at a time", KEY_TASK_GROUP_ID));
    }

    final String taskGroupId = args.get(0);
    try {
      return Response.ok(runtimeMaster.get().getTaskGroupInfo(taskGroupId));
    } catch (final TaskGroupNotFoundException e) {
      return Response.notFound(taskGroupId);
    } catch (final IOException e) {
      return Response.badRequest(e.getMessage());
    }
  }

  private Response taskGroupList(final Map<String, List<String>> queryMap) {
    final List<String> args = queryMap.get(KEY_STAGE_ID);
    if (args.size() != 1) {
      return Response.badRequest(String.format("Usage : only one %s at a time", KEY_STAGE_ID));
    }

    final String stageId = args.get(0);
    try {
      return Response.ok(runtimeMaster.get().getTaskGroupList(stageId));
    } catch (final StageNotFoundException e) {
      return Response.notFound(stageId);
    }
  }

  private Response onJobInfo() {
    try {
      return Response.ok(runtimeMaster.get().getJobInfo());
    } catch (IOException e) {
      return Response.badRequest(e.getMessage());
    }
  }
}
