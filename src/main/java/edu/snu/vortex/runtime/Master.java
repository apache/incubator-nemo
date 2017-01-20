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
import java.util.stream.IntStream;

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
    final List<List<Task>> taskGroup = convertToTaskGroups(stage);
    taskGroup.forEach(this::scheduleTaskGroup);
  }

  private List<List<Task>> convertToTaskGroups(final RtStage stage) {
    int parallelism = 5; // HACK
    int desiredByte = 20;
    final List<List<Task>> result = new ArrayList<>();

    for (final RtOperator rtOperator : stage.getTopoSorted()) {
      System.out.println("parallelism: " + parallelism);

      final Operator operator = rtOperator.getUserOp();
      if (operator instanceof Do) {
        // simply transform
        final Do doOperator = (Do) operator;
        final List<Task> taskList = result.stream()
            .map(list -> list.get(list.size()-1))
            .map(Task::getOutChan)
            .map(outChan -> new Task(outChan, (input -> (List)doOperator.transform(input, null)), new MemoryChannel()))
            .collect(Collectors.toList());

        if (taskList.size() != result.size()) {
          throw new RuntimeException(""+taskList.size());
        }


        IntStream.range(0, result.size()).forEach(i -> {
          result.get(i).add(taskList.get(i));
        });
        // merge taskList into result
      } else if (operator instanceof GroupByKey) {
        // partition to multi channel
        // final Iterable<KV> kvList = (Iterable<KV>)inChan.read();
      } else if (operator instanceof Broadcast) {
        throw new RuntimeException("Broadcast not yet supported");
      } else if (operator instanceof Source) {
        try {
          // simply read
          final Source sourceOperator = (Source) operator;
          final List<Source.Reader> readers = sourceOperator.getReaders(desiredByte);
          parallelism = readers.size(); // reset parallelism for this stage
          result.addAll(readers.stream()
              .map(this::convert)
              .map(task -> {
                final List<Task> newList = new ArrayList<>();
                newList.add(task);
                return newList;
              })
              .collect(Collectors.toList()));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        System.out.println("Source TaskList " + result);
      } else {
        throw new RuntimeException("Unknown operator");
      }
    }

    return result;
  }


  private Task convert(final Source.Reader reader) {
    final UserFunction userFunction = new UserFunction() {
      @Override
      public List func(List input) {
        try {
          return (List)reader.read();
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
