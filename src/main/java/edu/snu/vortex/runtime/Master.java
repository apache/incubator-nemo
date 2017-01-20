package edu.snu.vortex.runtime;

import edu.snu.vortex.compiler.ir.operator.*;
import edu.snu.vortex.runtime.common.*;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.snu.vortex.runtime.common.RtAttributes.CommPattern.SCATTER_GATHER;

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
    int desiredByte = 20; // HACK
    final List<List<Task>> result = new ArrayList<>();

    for (final RtOperator rtOperator : stage.getTopoSorted()) {
      final Operator operator = rtOperator.getUserOp();
      if (operator instanceof Do) {
        // simply transform
        final Do doOperator = (Do) operator;
        result.forEach(list -> {
          final Channel lastTaskOutChan = list.get(list.size()-1).getOutChans().get(0);
          final Task newTask = new DoTask(Arrays.asList(lastTaskOutChan), doOperator, Arrays.asList(new MemoryChannel()));
          list.add(newTask);
        });
      } else if (operator instanceof GroupByKey) {
        // partition to multi channel
        // final Iterable<KV> kvList = (Iterable<KV>)inChan.read();
      } else if (operator instanceof Source) {
        try {
          // simply read
          final Source sourceOperator = (Source) operator;
          final List<Source.Reader> readers = sourceOperator.getReaders(desiredByte);
          result.addAll(readers.stream()
              .map(reader -> new SourceTask(reader, Arrays.asList(new MemoryChannel())))
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

    final int reduceParallelism = 2; // hack
    stage.getOutputLinks().values().forEach(stageLink -> {
      if (stageLink.getRtOpLinks().stream()
          .anyMatch(operatorLink ->
              operatorLink.getRtOpLinkAttr().get(RtAttributes.RtOpLinkAttribute.COMM_PATTERN) == SCATTER_GATHER)) {

        result.forEach(list -> {
          final Channel lastTaskOutChan = list.get(list.size()-1).getOutChans().get(0);
          final List<Channel> newTaskOutChans = IntStream.range(0, reduceParallelism)
              .mapToObj(x -> x)
              .map(x -> new TCPChannel())
              .collect(Collectors.toList());
          list.add(new PartitionTask(lastTaskOutChan, newTaskOutChans));
        });
      }
    });

    return result;
  }

  private class SourceTask extends Task {
    private final Source.Reader reader;

    public SourceTask(final Source.Reader reader,
                      final List<Channel> outChans) {
      super(null, outChans);
      this.reader = reader;
    }

    @Override
    public void compute() {
      getOutChans().forEach(chan -> {
        try {
          chan.write((List) reader.read());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private class DoTask extends Task {
    private final Do doOperator;

    public DoTask(final List<Channel> inChans,
                  final Do doOperator,
                  final List<Channel> outChans) {
      super(inChans, outChans);
      this.doOperator = doOperator;
    }

    @Override
    public void compute() {
      getOutChans().get(0).write((List)doOperator.transform(getInChans().get(0).read(), null));
    }
  }

  private class PartitionTask extends Task {
    public PartitionTask(final Channel inChan,
                         final List<Channel> outChans) {
      super(Arrays.asList(inChan), outChans);
    }

    @Override
    public void compute() {
      final int numOfDsts = getOutChans().size();
      final List<KV> kvList = getInChans().get(0).read();
      final List<List<KV>> dsts = new ArrayList<>(numOfDsts);
      IntStream.range(0, numOfDsts).forEach(x -> dsts.add(new ArrayList<>()));
      kvList.forEach(kv -> {
        final int dst = Math.abs(kv.getKey().hashCode() % numOfDsts);
        dsts.get(dst).add(kv);
      });
      IntStream.range(0, numOfDsts).forEach(x -> getOutChans().get(x).write(dsts.get(x)));
    }
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
