package org.apache.nemo.runtime.executor.data.metadata;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Serializable;

/**
 * This class represents a metadata for a local file {@link org.apache.nemo.runtime.executor.data.block.Block}.
 * It resides in local only, and does not synchronize globally.
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class LocalFileMetadata<K extends Serializable> extends FileMetadata<K> {

  /**
   * Constructor.
   */
  public LocalFileMetadata() {
    super();
  }

  /**
   * @see FileMetadata#deleteMetadata()
   */
  @Override
  public void deleteMetadata() {
    // Do nothing because this metadata is only in the local memory.
  }

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public void commitBlock() {
    setCommitted(true);
  }
}
