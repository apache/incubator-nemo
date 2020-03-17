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

package org.apache.nemo.runtime.common.message;

import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.common.exception.UnknownFailureCauseException;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.state.TaskState;

import static org.apache.nemo.runtime.common.state.TaskState.State.COMPLETE;
import static org.apache.nemo.runtime.common.state.TaskState.State.ON_HOLD;

/**
 * Utility class for messages.
 */
public final class MessageUtils {
  /**
   * Private constructor for utility class.
   */
  private MessageUtils() {
  }

  public static TaskState.State convertTaskState(final ControlMessage.TaskStateFromExecutor state) {
    switch (state) {
      case READY:
        return TaskState.State.READY;
      case EXECUTING:
        return TaskState.State.EXECUTING;
      case COMPLETE:
        return COMPLETE;
      case FAILED_RECOVERABLE:
        return TaskState.State.SHOULD_RETRY;
      case FAILED_UNRECOVERABLE:
        return TaskState.State.FAILED;
      case ON_HOLD:
        return ON_HOLD;
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + state));
    }
  }

  public static ControlMessage.TaskStateFromExecutor convertState(final TaskState.State state) {
    switch (state) {
      case READY:
        return ControlMessage.TaskStateFromExecutor.READY;
      case EXECUTING:
        return ControlMessage.TaskStateFromExecutor.EXECUTING;
      case COMPLETE:
        return ControlMessage.TaskStateFromExecutor.COMPLETE;
      case SHOULD_RETRY:
        return ControlMessage.TaskStateFromExecutor.FAILED_RECOVERABLE;
      case FAILED:
        return ControlMessage.TaskStateFromExecutor.FAILED_UNRECOVERABLE;
      case ON_HOLD:
        return ControlMessage.TaskStateFromExecutor.ON_HOLD;
      default:
        throw new UnknownExecutionStateException(new Exception("This TaskState is unknown: " + state));
    }
  }

  public static TaskState.RecoverableTaskFailureCause convertFailureCause(
    final ControlMessage.RecoverableFailureCause cause) {
    switch (cause) {
      case InputReadFailure:
        return TaskState.RecoverableTaskFailureCause.INPUT_READ_FAILURE;
      case OutputWriteFailure:
        return TaskState.RecoverableTaskFailureCause.OUTPUT_WRITE_FAILURE;
      default:
        throw new UnknownFailureCauseException(
          new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }

  public static ControlMessage.RecoverableFailureCause convertFailureCause(
    final TaskState.RecoverableTaskFailureCause cause) {
    switch (cause) {
      case INPUT_READ_FAILURE:
        return ControlMessage.RecoverableFailureCause.InputReadFailure;
      case OUTPUT_WRITE_FAILURE:
        return ControlMessage.RecoverableFailureCause.OutputWriteFailure;
      default:
        throw new UnknownFailureCauseException(
          new Throwable("The failure cause for the recoverable failure is unknown"));
    }
  }
}
