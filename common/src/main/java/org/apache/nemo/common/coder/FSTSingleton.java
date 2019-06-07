package org.apache.nemo.common.coder;

import org.nustaq.serialization.FSTConfiguration;

public final class FSTSingleton {
  static FSTConfiguration singletonConf = FSTConfiguration.createDefaultConfiguration();
  public static FSTConfiguration getInstance() {
    return singletonConf;
  }
}
