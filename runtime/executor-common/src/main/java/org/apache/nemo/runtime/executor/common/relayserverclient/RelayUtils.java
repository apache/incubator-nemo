package org.apache.nemo.runtime.executor.common.relayserverclient;


public class RelayUtils {

  public static String createId(final String edgeId,
                                final int taskIndex,
                                final boolean inContext) {
    return new StringBuilder(edgeId)
      .append("#")
      .append(taskIndex)
      .append("#")
      .append(inContext ? "1" : "0")
      .toString();
  }
}
