package org.apache.nemo.runtime.executor.common.relayserverclient;

import java.io.ByteArrayOutputStream;

public class RelayUtils {


  public byte[] encodeDstId(final String dstId) {
    final byte[] bytes = new byte[30];
    final int l = dstId.length();
  }

  public static String createId(final String edgeId,
                                final int taskIndex,
                                final boolean inContext) {
    return new StringBuilder(edgeId)
      .append("#")
      .append(taskIndex)
      .append("#")
      .append(inContext ? "0" : "1")
      .toString();
  }
}
