package org.apache.nemo.offloading.common;

public final class TaskOffloadingEvent {

  public enum ControlType {
    SEND_TO_OFFLOADING_WORKER,
    OFFLOAD_DONE,
    DEOFFLOADING,
  }

  private final ControlType type;
  private final Object event;

  public TaskOffloadingEvent(final ControlType type,
                             final Object event) {
    this.type = type;
    this.event = event;
  }

  public ControlType getType() {
    return type;
  }

  public Object getEvent() {
    return event;
  }

}
