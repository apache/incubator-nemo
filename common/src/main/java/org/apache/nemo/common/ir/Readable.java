package org.apache.nemo.common.ir;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface for readable.
 * @param <O> output type.
 */
public interface Readable<O> extends Serializable {
  /**
   * Method to read data from the source.
   *
   * @return an {@link Iterable} of the data read by the readable.
   * @throws IOException exception while reading data.
   */
  Iterable<O> read() throws IOException;

  /**
   * Returns the list of locations where this readable resides.
   * Each location has a complete copy of the readable.
   *
   * @return List of locations where this readable resides
   * @throws UnsupportedOperationException when this operation is not supported
   * @throws Exception                     any other exceptions on the way
   */
  List<String> getLocations() throws Exception;
}
