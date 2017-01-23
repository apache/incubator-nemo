package edu.snu.vortex.runtime;

import edu.snu.vortex.runtime.executor.BlockManager;

import java.util.List;

public class MemoryChannel extends Channel {

  @Override
  public void write(List data) {
    BlockManager.getInstance().write(this, data);
  }

  @Override
  public List read() {
    return BlockManager.getInstance().read(this);
  }
}
