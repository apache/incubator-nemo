package org.apache.nemo.compiler.frontend.beam.source;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ReadableService {

  private static final ExecutorService INSTANCE = Executors.newFixedThreadPool(30);

  public static ExecutorService getInstance() {
    return INSTANCE;
  }
}
