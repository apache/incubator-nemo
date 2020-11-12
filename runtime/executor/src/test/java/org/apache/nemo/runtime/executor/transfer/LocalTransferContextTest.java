/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.transfer;

import junit.framework.TestCase;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.junit.Test;
import java.util.Iterator;

/**
 * Unit Test for {@link LocalOutputContext} and {@link LocalInputContext}.
 */
public final class LocalTransferContextTest extends TestCase {
  private static final String EXECUTOR_ID = "TEST_EXECUTOR";
  private static final String EDGE_ID = "DUMMY_EDGE";
  private static final int SRC_TASK_INDEX = 0;
  private static final int DST_TASK_INDEX = 0;
  private static final Serializer NULL_SERIALIZER = null;
  private static final int NUM_OF_ELEMENTS = 10000000;
  private static int expectedCount = 0;
  private static int count = 0;

  @Test
  public void testWriteAndRead() {
    // Initialize a local output context and its output stream
    final LocalOutputContext outputContext = new LocalOutputContext(EXECUTOR_ID, EDGE_ID, SRC_TASK_INDEX, DST_TASK_INDEX);
    final TransferOutputStream outputStream = outputContext.newOutputStream();

    // Initialize a local input context and its input iterator
    final LocalInputContext inputContext = new LocalInputContext(outputContext);
    final Iterator<Object> inputIterator = inputContext.getIterator();

    // Task of sending data
    final class sendingData implements Runnable {
      @Override
      public void run() {
        for (int element = 0; element < NUM_OF_ELEMENTS; element++) {
          expectedCount += element;
          // For every million elements, a thread rests for a second
          if (element % 1000000 == 0) {
            try {
              Thread.sleep(1000);
            }
            catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          // Send an element
          outputStream.writeElement(element, NULL_SERIALIZER);
        }
        // Close this context
        outputContext.close();
      }
    }

    // Task of retrieving data
    final class retrievingData implements Runnable {
      @Override
      public void run() {
        while (inputIterator.hasNext()) {
          int counter = 0;
          Object element = inputIterator.next();
          count += (int) element;
          counter++;
          if (counter % 500000 == 0) {
            try {
              Thread.sleep(500);
            }
            catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }

    // Spawn threads to run tasks
    final Thread receiver = new Thread(new retrievingData());
    final Thread sender = new Thread(new sendingData());

    // Execute tasks
    sender.start();
    receiver.start();

    // Wait until tasks are completed
    try {
      receiver.join();
      sender.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // Check if the receiver has received all the data successfully
    assertEquals(expectedCount, count);
    // Check whether both the local output context and the input context have been closed
    assertTrue(outputContext.isClosed());
    assertTrue(inputContext.isOutputContextClosed());
  }
}
