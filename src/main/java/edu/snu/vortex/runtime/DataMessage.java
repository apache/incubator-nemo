package edu.snu.vortex.runtime;

import java.io.Serializable;
import java.util.List;

public class DataMessage implements Serializable {
  private final String channelId;
  private final List data;

  public DataMessage(String channelId, List data) {
    this.channelId = channelId;
    this.data = data;
  }

  public String getChannelId() {
    return channelId;
  }

  public List getData() {
    return data;
  }
}
