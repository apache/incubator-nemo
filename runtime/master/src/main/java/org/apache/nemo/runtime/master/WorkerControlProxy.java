package org.apache.nemo.runtime.master;

import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import org.apache.nemo.offloading.common.EventHandler;
import org.apache.nemo.offloading.common.OffloadingMasterEvent;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.nemo.runtime.master.lambda.LambdaContainerRequester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nemo.offloading.common.OffloadingMasterEvent.Type.DUPLICATE_REQUEST_TERMIATION;
import static org.apache.nemo.runtime.master.WorkerControlProxy.State.*;

public final class WorkerControlProxy implements EventHandler<OffloadingMasterEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerControlProxy.class.getName());

  public enum State {
    DEACTIVATING,
    DEACTIVATE,
    ACTIVATE,
    ACTIVATING,
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
  private final LambdaContainerRequester.LambdaActivator activator;
  private final ClientRPC clientRPC;

  public WorkerControlProxy(final int requestId,
                            final String executorId,
                            final Channel controlChannel,
                            final ClientRPC clientRPC,
                            final LambdaContainerRequester.LambdaActivator activator,
                            final Set<WorkerControlProxy> pendingActivationWorkers) {
    this.requestId = requestId;
    this.executorId = executorId;
    this.clientRPC = clientRPC;
    this.controlChannel = controlChannel;
    this.pendingActivationWorkers = pendingActivationWorkers;
    this.activator = activator;
    this.state = new AtomicReference<>(State.ACTIVATE);
  }

  public String getExecutorId() {
    return er.getExecutorId();
  }

  public void setRepresentor(ExecutorRepresenter er) {
    this.er = er;
  }

  public ExecutorRepresenter getExecutorRepresenter() {
    return er;
  }

  public void setDataChannel(ExecutorRepresenter er,
                             final String fullAddr) {

    state.set(State.ACTIVATE);

    this.er = er;
    this.dataFullAddr = fullAddr;
  }

  public boolean isActive() {
    synchronized (state) {
      return state.get().equals(State.ACTIVATE);
    }
  }

  public boolean isActivating() {
    synchronized (state) {
      return state.get().equals(State.ACTIVATING);
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

  public void activate() {
    synchronized (state) {
      if (state.get().equals(DEACTIVATE)) {
        LOG.info("Send activate message for worker {}", requestId);
        state.set(ACTIVATING);
        synchronized (pendingActivationWorkers) {
          pendingActivationWorkers.add(this);
        }
        activator.activate();
      } else {
        throw new RuntimeException("Worker " + requestId + "/" + state +
          " is not deactive but try to activate");
      }
    }
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

  public boolean isDeactivated() {
    return state.get().equals(DEACTIVATE);
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

  private final List<Channel> duplicateRequestChannels = new LinkedList<>();

  @Override
  public void onNext(OffloadingMasterEvent msg) {
    switch (msg.getType()) {
      case DUPLICATE_REQUEST: {
        LOG.info("Duplicate request worker {}", requestId);
        synchronized (pendingActivationWorkers) {
          if (isActivating()) {
            // we should send another message
            if (duplicateRequestChannels.size() > 6) {
              LOG.info("Cannot acquire the same worker {}", requestId);
              duplicateRequestChannels.forEach(c ->
                c.writeAndFlush(new OffloadingMasterEvent(DUPLICATE_REQUEST_TERMIATION, new byte[0], 0)));
              duplicateRequestChannels.clear();
              msg.channel.writeAndFlush(new OffloadingMasterEvent(DUPLICATE_REQUEST_TERMIATION, new byte[0], 0));
              clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
                .setType(ControlMessage.DriverToClientMessageType.KillAll).build());
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              throw new RuntimeException("Cannot acquire the same worker " + requestId);
            }
            duplicateRequestChannels.add(msg.channel);
            activator.activate();
          } else {
            LOG.info("Send duplicate request termination to {}", requestId);
            msg.channel.writeAndFlush(new OffloadingMasterEvent(DUPLICATE_REQUEST_TERMIATION, new byte[0], 0));
          }
        }
        break;
      }
      case ACTIVATE: {
        LOG.info("Activated worker {}", requestId);

        synchronized (pendingActivationWorkers) {

          if (!pendingActivationWorkers.contains(this)) {
            LOG.info("This request is already processed.. should terminate the worker {}",
              requestId);
            controlChannel.writeAndFlush(new OffloadingMasterEvent(
              DUPLICATE_REQUEST_TERMIATION, null));
            //throw new RuntimeException("Pending activation worker does not contain " + requestId);
            return;
          }

          pendingActivationWorkers.remove(this);

          // flush dup request channels
          duplicateRequestChannels.forEach(dupChannel -> {
            dupChannel.writeAndFlush(new OffloadingMasterEvent(
              DUPLICATE_REQUEST_TERMIATION, null));
          });

          duplicateRequestChannels.clear();

          LOG.info("Set lambda worker {} to activate", requestId);
          state.set(State.ACTIVATE);
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
