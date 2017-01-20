package edu.snu.vortex.runtime;

import edu.snu.vortex.compiler.ir.operator.*;
import edu.snu.vortex.runtime.common.ExecutionPlan;
import edu.snu.vortex.runtime.common.RtAttributes;
import edu.snu.vortex.runtime.common.RtOperator;
import edu.snu.vortex.runtime.common.RtStage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Remote calls
 * - Send TaskGroup(List of tasks)
 * - Receive ChannelReadyMessage
 */
public class Master {
  final List<Executor> executors;
  int executorIndex;

  public Master() {
    executors = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      executors.add(new Executor(this));
    executorIndex = 0;
  }

  /////////////////////////////// Scheduling

  public void executeJob(final ExecutionPlan executionPlan) {
    final Set<RtStage> root = executionPlan.getNextRtStagesToExecute();
    root.forEach(this::executeStage);
  }

  private void executeStage(final RtStage stage) {
    final Set<List<Task>> taskGroup = convertToTaskGroups(stage);
    taskGroup.forEach(this::scheduleTaskGroup);
  }

  private Set<List<Task>> convertToTaskGroups(final RtStage stage) {
    int parallelism = (int)stage.getAttr(RtAttributes.RtStageAttribute.PARALLELISM);
    final List<List<Task>> result = new ArrayList<>();

    for (final RtOperator rtOperator : stage.getTopoSorted()) {
      final Operator operator = rtOperator.getUserOp();
      if (operator instanceof Do) {
        // simply transform
        final Do doOperator = (Do) operator;
        final List<Task> taskList = result.get(result.size()-1).stream()
            .map(Task::getOutChan)
            .map(outChan -> new Task(outChan, null, new MemoryChannel()))
            .collect(Collectors.toList());
        // merge taskList into result
      } else if (operator instanceof GroupByKey) {
        // partition to multi channel
        // final Iterable<KV> kvList = (Iterable<KV>)inChan.read();
      } else if (operator instanceof Broadcast) {
        throw new RuntimeException("Broadcast not yet supported")
      } else if (operator instanceof Source) {
        // simply read
        final Source sourceOperator = (Source)operator;
        final List<Source.Reader> readers = sourceOperator.getReaders(parallelism);
        parallelism = readers.size(); // reset parallelism for this stage
        result.add(readers.stream()
            .map(this::convert)
            .collect(Collectors.toList()));
      } else {
        throw new RuntimeException("Unknown operator");
      }
    }

    return null;
  }

  private Task convert(final Do doOperator, final Channel inChan) {
  }

  private Task convert(final Source.Reader reader) {
    final UserFunction userFunction = new UserFunction() {
      @Override
      public List func(List input) {
        try {
          return reader.read();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    return new Task(null, userFunction, new MemoryChannel());
  }

  private void scheduleTaskGroup(final List<Task> taskGroup) {
    // Round-robin executor pick
    final Executor executor = pickExecutor();
    executor.executeTaskGroup(taskGroup); // Remote call
  }

  private Executor pickExecutor() {
    executorIndex++;
    return executors.get(executorIndex % executors.size());
  }

  /////////////////////////////// Shuffle (Remote call)

  public void onRemoteChannelReady() {
    // executor.executeTask() <- make the receiver task read the data
  }

  public Executor getExecutor() {
    return null;
  }
}
