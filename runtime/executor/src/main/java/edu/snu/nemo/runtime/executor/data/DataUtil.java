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
package edu.snu.nemo.runtime.executor.data;

import com.google.common.io.CountingInputStream;
import edu.snu.nemo.common.DirectByteArrayOutputStream;
import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.streamchainer.StreamChainer;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for data handling (e.g., (de)serialization).
 */
public final class DataUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DataUtil.class.getName());

  /**
   * Empty constructor.
   */
  private DataUtil() {
    // Private constructor.
  }

  /**
   * Serializes the elements in a non-serialized partition into an output stream.
   *
   * @param coder                  the coder to encode the elements.
   * @param nonSerializedPartition the non-serialized partition to serialize.
   * @param bytesOutputStream      the output stream to write.
   * @return total number of elements in the partition.
   * @throws IOException if fail to serialize.
   */
  public static long serializePartition(final Coder coder,
                                        final NonSerializedPartition nonSerializedPartition,
                                        final OutputStream bytesOutputStream) throws IOException {
    long elementsCount = 0;
    for (final Object element : nonSerializedPartition.getData()) {
      coder.encode(element, bytesOutputStream);
      elementsCount++;
    }

    return elementsCount;
  }

  /**
   * Reads the data of a partition from an input stream and deserializes it.
   *
   * @param elementsInPartition the number of elements in this partition.
   * @param serializer          the serializer to decode the bytes.
   * @param key                 the key value of the result partition.
   * @param inputStream         the input stream which will return the data in the partition as bytes.
   * @param <K>                 the key type of the partitions.
   * @return the list of deserialized elements.
   * @throws IOException if fail to deserialize.
   */
  public static <K extends Serializable> NonSerializedPartition deserializePartition(final long elementsInPartition,
                                                            final Serializer serializer,
                                                            final K key,
                                                            final InputStream inputStream) throws IOException {
    final List deserializedData = new ArrayList();
    final InputStreamIterator iterator = new InputStreamIterator(Collections.singletonList(inputStream).iterator(),
        serializer, elementsInPartition);
    iterator.forEachRemaining(deserializedData::add);
    return new NonSerializedPartition(key, deserializedData, iterator.getNumSerializedBytes(),
        iterator.getNumEncodedBytes());
  }

  /**
   * Converts the non-serialized {@link edu.snu.nemo.runtime.executor.data.partition.Partition}s
   * in an iterable to serialized partitions.
   *
   * @param serializer          the serializer for serialization.
   * @param partitionsToConvert the partitions to convert.
   * @param <K>                 the key type of the partitions.
   * @return the converted {@link SerializedPartition}s.
   * @throws IOException if fail to convert.
   */
  public static <K extends Serializable> Iterable<SerializedPartition<K>> convertToSerPartitions(
      final Serializer serializer,
      final Iterable<NonSerializedPartition<K>> partitionsToConvert) throws IOException {
    final List<SerializedPartition<K>> serializedPartitions = new ArrayList<>();
    for (final NonSerializedPartition<K> partitionToConvert : partitionsToConvert) {
      try (
          final DirectByteArrayOutputStream bytesOutputStream = new DirectByteArrayOutputStream();
          final OutputStream wrappedStream = buildOutputStream(bytesOutputStream, serializer.getStreamChainers());
      ) {
        final long elementsTotal = serializePartition(serializer.getCoder(), partitionToConvert, wrappedStream);
        // We need to close wrappedStream on here, because DirectByteArrayOutputStream:getBufDirectly() returns
        // inner buffer directly, which can be an unfinished(not flushed) buffer.
        wrappedStream.close();
        final byte[] serializedBytes = bytesOutputStream.getBufDirectly();
        final int actualLength = bytesOutputStream.getCount();
        serializedPartitions.add(
            new SerializedPartition<>(partitionToConvert.getKey(), elementsTotal, serializedBytes, actualLength));
      }
    }
    return serializedPartitions;
  }

  /**
   * Converts the serialized {@link edu.snu.nemo.runtime.executor.data.partition.Partition}s
   * in an iterable to non-serialized partitions.
   *
   * @param serializer          the serializer for deserialization.
   * @param partitionsToConvert the partitions to convert.
   * @param <K>                 the key type of the partitions.
   * @return the converted {@link NonSerializedPartition}s.
   * @throws IOException if fail to convert.
   */
  public static <K extends Serializable> Iterable<NonSerializedPartition<K>> convertToNonSerPartitions(
      final Serializer serializer,
      final Iterable<SerializedPartition<K>> partitionsToConvert) throws IOException {
    final List<NonSerializedPartition<K>> nonSerializedPartitions = new ArrayList<>();
    for (final SerializedPartition<K> partitionToConvert : partitionsToConvert) {
      final K key = partitionToConvert.getKey();
      try (final ByteArrayInputStream byteArrayInputStream =
               new ByteArrayInputStream(partitionToConvert.getData())) {
        final NonSerializedPartition<K> deserializePartition = deserializePartition(
            partitionToConvert.getElementsCount(), serializer, key, byteArrayInputStream);
        nonSerializedPartitions.add(deserializePartition);
      }
    }
    return nonSerializedPartitions;
  }

  /**
   * Converts a block id to the corresponding file path.
   *
   * @param blockId       the ID of the block.
   * @param fileDirectory the directory of the target block file.
   * @return the file path of the partition.
   */
  public static String blockIdToFilePath(final String blockId,
                                         final String fileDirectory) {
    return fileDirectory + "/" + blockId;
  }

  /**
   * Converts a block id to the corresponding metadata file path.
   *
   * @param blockId       the ID of the block.
   * @param fileDirectory the directory of the target block file.
   * @return the metadata file path of the partition.
   */
  public static String blockIdToMetaFilePath(final String blockId,
                                             final String fileDirectory) {
    return fileDirectory + "/" + blockId + "_meta";
  }

  /**
   * Concatenates an iterable of non-serialized {@link edu.snu.nemo.runtime.executor.data.partition.Partition}s
   * into a single iterable of elements.
   *
   * @param partitionsToConcat the partitions to concatenate.
   * @return the concatenated iterable of all elements.
   * @throws IOException if fail to concatenate.
   */
  public static Iterable concatNonSerPartitions(final Iterable<NonSerializedPartition> partitionsToConcat)
      throws IOException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (final NonSerializedPartition nonSerializedPartition : partitionsToConcat) {
      final Iterable elementsInPartition = nonSerializedPartition.getData();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(elementsInPartition.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }

  /**
   * An iterator that emits objects from {@link InputStream} using the corresponding {@link Coder}.
   * @param <T> The type of elements.
   */
  public static final class InputStreamIterator<T> implements IteratorWithNumBytes<T> {

    private final Iterator<InputStream> inputStreams;
    private final Serializer<T> serializer;
    private final long limit;

    private volatile CountingInputStream serializedCountingStream = null;
    private volatile CountingInputStream encodedCountingStream = null;
    private volatile boolean hasNext = false;
    private volatile T next;
    private volatile boolean cannotContinueDecoding = false;
    private volatile long elementsDecoded = 0;
    private volatile long numSerializedBytes = 0;
    private volatile long numEncodedBytes = 0;

    /**
     * Construct {@link Iterator} from {@link InputStream} and {@link Coder}.
     *
     * @param inputStreams The streams to read data from.
     * @param serializer   The serializer.
     */
    public InputStreamIterator(final Iterator<InputStream> inputStreams, final Serializer<T> serializer) {
      this.inputStreams = inputStreams;
      this.serializer = serializer;
      // -1 means no limit.
      this.limit = -1;
    }

    /**
     * Construct {@link Iterator} from {@link InputStream} and {@link Coder}.
     *
     * @param inputStreams The streams to read data from.
     * @param serializer   The serializer.
     * @param limit        The number of elements from the {@link InputStream}.
     */
    public InputStreamIterator(
        final Iterator<InputStream> inputStreams,
        final Serializer<T> serializer,
        final long limit) {
      if (limit < 0) {
        throw new IllegalArgumentException("Negative limit not allowed.");
      }
      this.inputStreams = inputStreams;
      this.serializer = serializer;
      this.limit = limit;
    }

    @Override
    public boolean hasNext() {
      if (hasNext) {
        return true;
      }
      if (cannotContinueDecoding) {
        return false;
      }
      if (limit != -1 && limit == elementsDecoded) {
        cannotContinueDecoding = true;
        return false;
      }
      while (true) {
        try {
          if (encodedCountingStream == null) {
            if (inputStreams.hasNext()) {
              serializedCountingStream = new CountingInputStream(inputStreams.next());
              encodedCountingStream = new CountingInputStream(buildInputStream(
                  serializedCountingStream, serializer.getStreamChainers()));
            } else {
              cannotContinueDecoding = true;
              return false;
            }
          }
        } catch (final IOException e) {
          // We cannot recover IOException thrown by buildInputStream.
          throw new RuntimeException(e);
        }
        try {
          next = serializer.getCoder().decode(encodedCountingStream);
          hasNext = true;
          elementsDecoded++;
          return true;
        } catch (final IOException e) {
          // IOException from decoder indicates EOF event.
          numSerializedBytes += serializedCountingStream.getCount();
          numEncodedBytes += encodedCountingStream.getCount();
          serializedCountingStream = null;
          encodedCountingStream = null;
        }
      }
    }

    @Override
    public T next() {
      if (hasNext()) {
        final T element = next;
        next = null;
        hasNext = false;
        return element;
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public long getNumSerializedBytes() {
      if (hasNext()) {
        throw new IllegalStateException("Iteration not completed.");
      }
      return numSerializedBytes;
    }

    @Override
    public long getNumEncodedBytes() {
      if (hasNext()) {
        throw new IllegalStateException("Iteration not completed.");
      }
      return numEncodedBytes;
    }
  }

  /**
   * Chain {@link InputStream} with {@link StreamChainer}s.
   *
   * @param in             the {@link InputStream}.
   * @param streamChainers the list of {@link StreamChainer} to be applied on the stream.
   * @return chained       {@link InputStream}.
   * @throws IOException   if fail to create new stream.
   */
  public static InputStream buildInputStream(final InputStream in, final List<StreamChainer> streamChainers)
  throws IOException {
    InputStream chained = in;
    for (final StreamChainer streamChainer : streamChainers) {
      chained = streamChainer.chainInput(chained);
    }
    return chained;
  }

  /**
   * Chain {@link OutputStream} with {@link StreamChainer}s.
   *
   * @param out            the {@link OutputStream}.
   * @param streamChainers the list of {@link StreamChainer} to be applied on the stream.
   * @return chained       {@link OutputStream}.
   * @throws IOException   if fail to create new stream.
   */
  public static OutputStream buildOutputStream(final OutputStream out, final List<StreamChainer> streamChainers)
  throws IOException {
    OutputStream chained = out;
    final List<StreamChainer> temporaryStreamChainerList = new ArrayList<>(streamChainers);
    Collections.reverse(temporaryStreamChainerList);
    for (final StreamChainer streamChainer : temporaryStreamChainerList) {
      chained = streamChainer.chainOutput(chained);
    }
    return chained;
  }

  /**
   * {@link Iterator} with interface to access to the number of bytes.
   * @param <T> the type of decoded object
   */
  public interface IteratorWithNumBytes<T> extends Iterator<T> {
    /**
     * Create an {@link IteratorWithNumBytes}, with no information about the number of bytes.
     * @param innerIterator {@link Iterator} to wrap
     * @param <E> the type of decoded object
     * @return an {@link IteratorWithNumBytes}, with no information about the number of bytes
     */
    static <E> IteratorWithNumBytes<E> of(final Iterator<E> innerIterator) {
      return new IteratorWithNumBytes<E>() {
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
     * @param innerIterator {@link Iterator} to wrap
     * @param numSerializedBytes the number of bytes in serialized form
     * @param numEncodedBytes the number of bytes in encoded form
     * @param <E> the type of decoded object
     * @return an {@link IteratorWithNumBytes}, with the information about the number of bytes
     */
    static <E> IteratorWithNumBytes<E> of(final Iterator<E> innerIterator,
                                          final long numSerializedBytes,
                                          final long numEncodedBytes) {
      return new IteratorWithNumBytes<E>() {
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
     * @return the number of bytes in serialized form (which is, for example, encoded and compressed)
     * @throws NumBytesNotSupportedException when the operation is not supported
     * @throws IllegalStateException         when the information is not ready
     */
    long getNumSerializedBytes() throws NumBytesNotSupportedException;

    /**
     * This method should be called after the actual data is taken out of iterator,
     * since the existence of an iterator does not guarantee that data inside it is ready.
     * @return the number of bytes in encoded form (which is ready to be decoded)
     * @throws NumBytesNotSupportedException when the operation is not supported
     * @throws IllegalStateException         when the information is not ready
     */
    long getNumEncodedBytes() throws NumBytesNotSupportedException;
  }
}
