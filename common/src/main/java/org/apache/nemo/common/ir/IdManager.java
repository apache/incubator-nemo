package org.apache.nemo.common.ir;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * ID manager.
 */
public final class IdManager {
  /**
   * Private constructor.
   */
  private IdManager() {
  }

  private static AtomicInteger vertexId = new AtomicInteger(1);
  private static AtomicInteger edgeId = new AtomicInteger(1);
  private static volatile boolean isDriver = false;

  /**
   * @return a new operator ID.
   */
  public static String newVertexId() {
    return "vertex" + (isDriver ? "(d)" : "") + vertexId.getAndIncrement();
  }

  /**
   * @return a new edge ID.
   */
  public static String newEdgeId() {
    return "edge" + (isDriver ? "(d)" : "") + edgeId.getAndIncrement();
  }

  /**
   * Set the realm of the loaded class as REEF driver.
   */
  public static void setInDriver() {
    isDriver = true;
  }
}
