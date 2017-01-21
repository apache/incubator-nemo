package edu.snu.vortex.runtime;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Channel<T> {
  private static AtomicInteger generator = new AtomicInteger(1);

  private final String id = "Channel-" + generator.getAndIncrement();

  public String getId() {
    return id;
  }

  public abstract void write(List<T> data);

  public abstract List<T> read();

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "#" + id;
  }
}
