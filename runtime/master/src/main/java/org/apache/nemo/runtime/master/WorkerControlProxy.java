package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nemo.runtime.master.WorkerControlProxy.State.ACTIVATE;
import static org.apache.nemo.runtime.master.WorkerControlProxy.State.DEACTIVATING;

public final class WorkerControlProxy implements EventHandler<OffloadingMasterEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerControlProxy.class.getName());

  public enum State {
    DEACTIVATING,
    DEACTIVATE,
    ACTIVATE,
    READY,
  }

  private final int requestId;
  private final Channel controlChannel;
  private final AtomicReference<State> state;
  private final String executorId;

  private ExecutorRepresenter er;
  private String dataFullAddr;

  private long lastActivationTime = System.currentTimeMillis();

  public final Set<String> readyTasks = new HashSet<>();
  public final Set<String> pendingTasks = new HashSet<>();

  private final Set<WorkerControlProxy> pendingActivationWorkers;

  public WorkerControlProxy(final int requestId,
                            final String executorId,
                            final Channel controlChannel,
                            final Set<WorkerControlProxy> pendingActivationWorkers) {
    this.requestId = requestId;
    this.executorId = executorId;
    this.controlChannel = controlChannel;
    this.pendingActivationWorkers = pendingActivationWorkers;
    this.state = new AtomicReference<>(State.READY);
  }

  public String getExecutorId() {
    return er.getExecutorId();
  }

  public void setDataChannel(ExecutorRepresenter er,
                             final String fullAddr) {

    synchronized (state) {
      state.set(State.ACTIVATE);
    }

    this.er = er;
    this.dataFullAddr = fullAddr;

  }

  public boolean isActive() {
    synchronized (state) {
      return state.get().equals(State.ACTIVATE);
    }
  }

  public int getId() {
    return requestId;
  }

  public Channel getControlChannel() {
    return controlChannel;
  }

  public boolean reclaimed() {
    return !(controlChannel.isActive() && controlChannel.isOpen());
  }

  public void deactivate() {
    synchronized (state) {
      if (state.get().equals(ACTIVATE)) {
        state.set(DEACTIVATING);
        LOG.info("Send end message for deactivating worker {}", requestId);
        controlChannel
          .writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, null));
      } else {
        throw new RuntimeException("Worker " + requestId + "/" + state +
          " is not active but try to deactivate");
      }
    }
  }

  public boolean allPendingTasksReady() {
    synchronized (pendingTasks) {
      synchronized (readyTasks) {
        return pendingTasks.isEmpty() && !readyTasks.isEmpty();
      }
    }
  }

  @Override
  public String toString() {
    return requestId + "/" + executorId;
  }

  public void sendTask(final String taskId, final OffloadingMasterEvent event) {
    synchronized (pendingTasks) {
      pendingTasks.add(taskId);
    }
    controlChannel.writeAndFlush(event);
  }


  @Override
  public void onNext(OffloadingMasterEvent msg) {
    switch (msg.getType()) {
      case ACTIVATE: {
        LOG.info("Activated worker {}", requestId);

        synchronized (pendingActivationWorkers) {

          if (!pendingActivationWorkers.contains(this)) {
            LOG.info("This request is already processed.. should terminate the worker {}",
              requestId);
            controlChannel.writeAndFlush(new OffloadingMasterEvent(
              OffloadingMasterEvent.Type.DUPLICATE_REQUEST_TERMIATION, null));
            //throw new RuntimeException("Pending activation worker does not contain " + requestId);
            return;
          }

          pendingActivationWorkers.remove(this);

          synchronized (state) {
            state.set(State.ACTIVATE);
          }
        }
        break;
      }
      case TASK_READY: {
        try {
          final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
          final String taskId = bis.readUTF();
          bis.close();

          LOG.info("Task ready for worker {}: {}", requestId, taskId);
          synchronized (pendingTasks) {
            synchronized (readyTasks) {
              pendingTasks.remove(taskId);
              readyTasks.add(taskId);
            }
          }
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        break;
      }
      case CPU_LOAD: {
        final double load = msg.getByteBuf().readDouble();
        LOG.info("Receive cpu load {} for worker ", load, requestId);
        // cpuAverage.addValue(load);
        break;
      }
      case END:
        LOG.info("Deactivating worker {}", requestId);

        lastActivationTime = System.currentTimeMillis();

        synchronized (state) {
          if (!state.get().equals(DEACTIVATING)) {
            LOG.warn("This worker {} is deactivated by executor ", requestId);
          }

          state.set(State.DEACTIVATE);
        }
        msg.getByteBuf().release();
        // endQueue.add(msg);
        break;
      default:
        throw new RuntimeException("Invalid type: " + msg);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerControlProxy that = (WorkerControlProxy) o;
    return requestId == that.requestId &&
      Objects.equals(controlChannel, that.controlChannel);
  }

  @Override
  public int hashCode() {

    return Objects.hash(requestId, controlChannel);
  }
}
