package org.apache.nemo.offloading.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class SharedCachedPool {

  public static final ExecutorService POOL = Executors.newCachedThreadPool();
}
