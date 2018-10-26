package org.apache.nemo.runtime.master;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Broadcast variables saved in the master.
 */
public final class BroadcastManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(BroadcastManagerMaster.class.getName());
  private static final Map<Serializable, Object> ID_TO_VARIABLE = new HashMap<>();

  private BroadcastManagerMaster() {
  }

  /**
   * @param variables from the client.
   */
  public static void registerBroadcastVariablesFromClient(final Map<Serializable, Object> variables) {
    LOG.info("Registered broadcast variable ids {} sent from the client", variables.keySet());
    ID_TO_VARIABLE.putAll(variables);
  }

  /**
   * @param id of the broadcast variable.
   * @return the requested broadcast variable.
   */
  public static Object getBroadcastVariable(final Serializable id) {
    return ID_TO_VARIABLE.get(id);
  }
}
