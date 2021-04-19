package org.apache.nemo.runtime.master;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

public final class PendingRedirectionTasks {
  public final Set<String> pendingRedirectionTasks = new HashSet<>();

  @Inject
  private PendingRedirectionTasks() {

  }
}
