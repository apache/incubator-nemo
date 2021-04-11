package org.apache.nemo.runtime.executor;

import org.apache.nemo.runtime.executor.common.datatransfer.InputPipeRegister;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.junit.Test;

import java.util.List;

public final class ControlEventHandlerTest {

  @Test
  public void testTaskStopByMaster() {

  }

  final class TestInputPipeRegister implements InputPipeRegister {

    @Override
    public void retrieveIndexForOffloadingSource(String srcTaskId, String edgeId) {

    }

    @Override
    public void registerInputPipe(String srcTaskId, String edgeId, String dstTaskId, InputReader inputReader) {

    }

    @Override
    public void sendSignalForInputPipes(List<String> srcTasks, String edgeId, String dstTaskId) {

    }

    @Override
    public void receiveAckInputStopSignal(String taskId, int pipeIndex) {

    }

    @Override
    public InputPipeState getInputPipeState(String taskId) {
      return null;
    }

    @Override
    public boolean isInputPipeStopped(String taskId) {
      return false;
    }

  }
}
