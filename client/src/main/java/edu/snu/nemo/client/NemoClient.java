/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.client;

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
