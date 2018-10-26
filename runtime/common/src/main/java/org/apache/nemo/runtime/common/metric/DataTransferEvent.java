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
