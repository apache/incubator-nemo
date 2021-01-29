package org.apache.nemo.runtime.executor.task.util;

import java.io.Serializable;

public final class EventOrWatermark implements Serializable {
    public Serializable event;
    public long watermark;
    private final boolean data;

    public EventOrWatermark(final Serializable event) {
      this.event = event;
      this.data = true;
    }

    public EventOrWatermark(final long watermark,
                            final boolean watermarked) {
      this.watermark = watermark;
      this.data = false;
    }

    public boolean isWatermark() {
      return !data;
    }
  }
