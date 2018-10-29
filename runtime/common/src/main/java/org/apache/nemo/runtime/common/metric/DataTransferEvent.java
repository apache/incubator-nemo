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
package org.apache.nemo.runtime.common.metric;

/**
 * Event for data transfer, such as data read or write.
 */
public class DataTransferEvent extends Event {
  private TransferType transferType;

  public DataTransferEvent(final long timestamp, final TransferType transferType) {
    super(timestamp);
    this.transferType = transferType;
  }

  /**
   * Get transfer type.
   * @return TransferType.
   */
  public final TransferType getTransferType() {
    return transferType;
  }

  /**
   * Set transfer type.
   * @param transferType TransferType to set.
   */
  public final void setTransferType(final TransferType transferType) {
    this.transferType = transferType;
  }

  /**
   * Enum of transfer types.
   */
  public enum TransferType {
    READ_START,
    READ_END,
    WRITE_START,
    WRITE_END
  }
}
