package org.apache.nemo.runtime.executor.common.datatransfer;

import java.util.Iterator;

/**
 * {@link Iterator} with interface to access to the number of bytes.
 *
 * @param <T> the type of decoded object
 */
public interface IteratorWithNumBytes<T> extends Iterator<T> {
  /**
   * Create an {@link IteratorWithNumBytes}, with no information about the number of bytes.
   *
   * @return an {@link IteratorWithNumBytes}, with no information about the number of bytes
   */

  boolean isFinished();

  static <E> IteratorWithNumBytes<E> of(final Iterator<E> innerIterator) {
    return new IteratorWithNumBytes<E>() {
      @Override
      public boolean isFinished() {
        return false;
      }

      @Override
      public long getNumSerializedBytes() throws NumBytesNotSupportedException {
        throw new NumBytesNotSupportedException();
      }

      @Override
      public long getNumEncodedBytes() throws NumBytesNotSupportedException {
        throw new NumBytesNotSupportedException();
      }

      @Override
      public boolean hasNext() {
        return innerIterator.hasNext();
      }

      @Override
      public E next() {
        return innerIterator.next();
      }
    };

  }

  /**
   * Create an {@link IteratorWithNumBytes}, with the number of bytes in decoded and serialized form.
   *
   * @param innerIterator      {@link Iterator} to wrap
   * @param numSerializedBytes the number of bytes in serialized form
   * @param numEncodedBytes    the number of bytes in encoded form
   * @param <E>                the type of decoded object
   * @return an {@link IteratorWithNumBytes}, with the information about the number of bytes
   */
  static <E> IteratorWithNumBytes<E> of(final Iterator<E> innerIterator,
                                        final long numSerializedBytes,
                                        final long numEncodedBytes) {
    return new IteratorWithNumBytes<E>() {
      @Override
      public boolean isFinished() {
        return false;
      }

      @Override
      public long getNumSerializedBytes() {
        return numSerializedBytes;
      }

      @Override
      public long getNumEncodedBytes() {
        return numEncodedBytes;
      }

      @Override
      public boolean hasNext() {
        return innerIterator.hasNext();
      }

      @Override
      public E next() {
        return innerIterator.next();
      }
    };
  }

  /**
   * Exception indicates {@link #getNumSerializedBytes()} or {@link #getNumEncodedBytes()} is not supported.
   */
  final class NumBytesNotSupportedException extends Exception {
    /**
     * Creates {@link NumBytesNotSupportedException}.
     */
    public NumBytesNotSupportedException() {
      super("Getting number of bytes is not supported");
    }
  }

  /**
   * This method should be called after the actual data is taken out of iterator,
   * since the existence of an iterator does not guarantee that data inside it is ready.
   *
   * @return the number of bytes in serialized form (which is, for example, encoded and compressed)
   * @throws NumBytesNotSupportedException when the operation is not supported
   * @throws IllegalStateException         when the information is not ready
   */
  long getNumSerializedBytes() throws NumBytesNotSupportedException;

  /**
   * This method should be called after the actual data is taken out of iterator,
   * since the existence of an iterator does not guarantee that data inside it is ready.
   *
   * @return the number of bytes in encoded form (which is ready to be decoded)
   * @throws NumBytesNotSupportedException when the operation is not supported
   * @throws IllegalStateException         when the information is not ready
   */
  long getNumEncodedBytes() throws NumBytesNotSupportedException;
}
