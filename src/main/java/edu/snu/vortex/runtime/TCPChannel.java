package edu.snu.vortex.runtime;

import edu.snu.vortex.runtime.executor.BlockManager;

import java.util.List;

public class TCPChannel extends Channel {

  public TCPChannel() {
  }

  @Override
  public void write(List data) {
    BlockManager.getInstance().write(this, data);
    BlockManager.getInstance().sendChannelReady(this.getId());
  }

  @Override
  public List read() {
    return BlockManager.getInstance().read(this);
  }
}
