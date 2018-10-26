package org.apache.nemo.runtime.master;

/**
 * Handler for aggregating data used in dynamic optimization.
 */
public interface DynOptDataHandler {
  /**
   * Updates data for dynamic optimization sent from Tasks.
   * @param dynOptData data used for dynamic optimization.
   */
  void updateDynOptData(Object dynOptData);

  /**
   * Returns aggregated data for dynamic optimization.
   * @return aggregated data used for dynamic optimization.
   */
  Object getDynOptData();
}
