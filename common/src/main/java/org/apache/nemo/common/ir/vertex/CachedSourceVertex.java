package org.apache.nemo.common.ir.vertex;

import org.apache.nemo.common.ir.Readable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bounded source vertex for cached data.
 * It does not have actual data but just wraps the cached input data.
 * @param <T> the type of data to emit.
 */
public final class CachedSourceVertex<T> extends SourceVertex<T> {
  private List<Readable<T>> readables;

  /**
   * Constructor.
   *
   * @param numPartitions the number of partitions.
   */
  public CachedSourceVertex(final int numPartitions) {
    this.readables = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      readables.add(new CachedReadable());
    }
  }

  /**
   * Constructor for cloning.
   *
   * @param readables the list of Readables to set.
   */
  private CachedSourceVertex(final List<Readable<T>> readables) {
    this.readables = readables;
  }

  @Override
  public CachedSourceVertex getClone() {
    final CachedSourceVertex that = new CachedSourceVertex<>(this.readables);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  @Override
  public List<Readable<T>> getReadables(final int desiredNumOfSplits) {
    // Ignore the desired number of splits.
    return readables;
  }

  @Override
  public void clearInternalStates() {
    readables = null;
  }

  /**
   * A Readable wrapper for cached data.
   * It does not contain any actual data but the data will be sent from the cached store through external input reader.
   */
  private final class CachedReadable implements Readable<T> {

    /**
     * Constructor.
     */
    private CachedReadable() {
      // Do nothing
    }

    @Override
    public Iterable<T> read() throws IOException {
      return Collections.emptyList();
    }

    @Override
    public List<String> getLocations() {
      throw new UnsupportedOperationException();
    }
  }
}
