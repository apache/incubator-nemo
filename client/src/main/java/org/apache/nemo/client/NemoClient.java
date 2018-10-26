package org.apache.nemo.client;

import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.client.JobMessage;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;

/**
 * A wrapper class that contains client handlers.
 */
@Unit
@ClientSide
public final class NemoClient {

  /**
   * Empty Constructor.
   */
  @Inject
  private NemoClient() {
    // empty
  }

  /**
   * Message handler.
   */
  public final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      final String msg = new String(message.get(), StandardCharsets.UTF_8);
      System.out.print(msg);
    }
  }
}
