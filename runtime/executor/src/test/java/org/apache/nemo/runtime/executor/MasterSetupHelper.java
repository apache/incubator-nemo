package org.apache.nemo.runtime.executor;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;

import org.apache.nemo.runtime.master.PipeManagerMaster;

import org.apache.reef.io.network.naming.NameServer;

import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;


import java.util.*;
import java.util.stream.Collectors;

public class MasterSetupHelper {
  private static final Tang TANG = Tang.Factory.getTang();

    public final Map<Triple<String, String, String>, Integer> pipeIndexMap = new HashMap<>();
    public final Map<String, String> taskScheduledMap = new HashMap<>();
    public final PipeManagerMaster pipeManagerMaster;
    public final NameServer nameServer;
    public final MessageEnvironment messageEnvironment;
    public final  List<String> executorIds = new LinkedList<>();
    public final MasterHandler masterHandler = new MasterHandler();

  public MasterSetupHelper() throws InjectionException {
    this.nameServer = PipeManagerTestHelper.createNameServer();
    final Injector injector = TANG.newInjector(
      PipeManagerTestHelper.createPipeManagerMasterConf(nameServer));

    this.pipeManagerMaster = injector.getInstance(PipeManagerMaster.class);
    this.messageEnvironment = injector.getInstance(MessageEnvironment.class);

    messageEnvironment.setupListener(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID,
      masterHandler);

    messageEnvironment.setupListener(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID,
      masterHandler);

    messageEnvironment.setupListener(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID,
      masterHandler);
  }

  final class MasterHandler implements MessageListener<ControlMessage.Message> {


    @Override
    public void onMessage(ControlMessage.Message message) {
      System.out.println("Message received '" + message.getType());
      // throw new RuntimeException("Not supported " + message.getType());
    }

    @Override
    public void onMessageWithContext(ControlMessage.Message message, MessageContext messageContext) {
      switch (message.getType()) {
        case CurrentExecutor: {
            messageContext.reply(
              ControlMessage.Message.newBuilder()
                .setId(RuntimeIdManager.generateMessageId())
                .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.CurrentExecutor)
                .addAllCurrExecutors(executorIds)
                .build());
          break;
        }
        case CurrentScheduledTask: {
          final Collection<String> c = taskScheduledMap
            .entrySet()
            .stream()
            .map(entry -> entry.getKey() + "," + entry.getValue())
            .collect(Collectors.toList());

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_SCHEDULE_MAP_LISTENER_ID)
              .setType(ControlMessage.MessageType.CurrentScheduledTask)
              .addAllCurrScheduledTasks(c)
              .build());
          break;
        }
        case RequestTaskIndex: {
          System.out.println(message.getRequestTaskIndexMsg());
          final ControlMessage.RequestTaskIndexMessage requestTaskIndexMessage =
            message.getRequestTaskIndexMsg();

          final String srcTaskId = requestTaskIndexMessage.getSrcTaskId();
          final String edgeId = requestTaskIndexMessage.getEdgeId();
          final String dstTaskId = requestTaskIndexMessage.getDstTaskId();
          final Triple<String, String, String> key = Triple.of(srcTaskId, edgeId, dstTaskId);

          messageContext.reply(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.TASK_INDEX_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.TaskIndexInfo)
              .setTaskIndexInfoMsg(ControlMessage.TaskIndexInfoMessage.newBuilder()
                .setRequestId(message.getId())
                .setTaskIndex(pipeIndexMap.get(key))
                .build())
              .build());
          break;
        }
        default:
          throw new RuntimeException("Not supported " + message.getType());
      }
    }
  }
}
