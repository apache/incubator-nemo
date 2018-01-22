package edu.snu.coral.runtime.executor.data;

import edu.snu.coral.common.DirectByteArrayOutputStream;
import edu.snu.coral.common.coder.Coder;
import edu.snu.coral.runtime.executor.data.chainable.Chainable;
import edu.snu.coral.runtime.executor.data.chainable.Serializer;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for data handling (e.g., (de)serialization).
 */
public final class DataUtil {
  /**
   * Empty constructor.
   */
  private DataUtil() {
    // Private constructor.
  }

  /**
   * Serializes the elements in a non-serialized partition into an output stream.
   *
   * @param serializer             the serializer to encode the elements.
   * @param nonSerializedPartition the non-serialized partition to serialize.
   * @param bytesOutputStream      the output stream to write.
   * @return total number of elements in the partition.
   * @throws IOException if fail to serialize.
   */
  public static long serializePartition(final Serializer serializer,
                                        final NonSerializedPartition nonSerializedPartition,
                                        final ByteArrayOutputStream bytesOutputStream) throws IOException {
    long elementsCount = 0;
    final Coder coder = serializer.getCoder();
    final OutputStream wrappedStream = buildOutputStream(bytesOutputStream, serializer.getChainables());
    for (final Object element : nonSerializedPartition.getData()) {
      coder.encode(element, wrappedStream);
      elementsCount++;
    }
    wrappedStream.close();

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
    (new InputStreamIterator(Collections.singletonList(inputStream).iterator(), coder, elementsInPartition))
        .forEachRemaining(deserializedData::add);
    return new NonSerializedPartition(key, deserializedData);
  }

  /**
   * Converts the non-serialized {@link Partition}s in an iterable to serialized {@link Partition}s.
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
      try (final DirectByteArrayOutputStream bytesOutputStream = new DirectByteArrayOutputStream()) {
        final long elementsTotal = serializePartition(serializer, partitionToConvert, bytesOutputStream);
        final byte[] serializedBytes = bytesOutputStream.getBufDirectly();
        final int actualLength = bytesOutputStream.getCount();
        serializedPartitions.add(
            new SerializedPartition<>(partitionToConvert.getKey(), elementsTotal, serializedBytes, actualLength));
      }
    }
    return serializedPartitions;
  }

  /**
   * Converts the serialized {@link Partition}s in an iterable to non-serialized {@link Partition}s.
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
            partitionToConvert.getElementsTotal(), serializer, key, byteArrayInputStream);
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
   * Concatenates an iterable of non-serialized {@link Partition}s into a single iterable of elements.
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
  public static final class InputStreamIterator<T> implements Iterator<T> {

    private final Iterator<InputStream> inputStreams;
    private final Serializer<T> serializer;
    private final long limit;

    private volatile InputStream currentInputStream = null;
    private volatile boolean hasNext = false;
    private volatile T next;
    private volatile boolean cannotContinueDecoding = false;
    private volatile long elementsDecoded = 0;

    /**
     * Construct {@link Iterator} from {@link InputStream} and {@link Coder}.
     *
     * @param inputStreams The streams to read data from.
     * @param serializer  The serializer.
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
     * @param serializer  The serializer.
     * @param limit        The number of elements from the {@link InputStream}.
     */
    public InputStreamIterator(final Iterator<InputStream> inputStreams, final Serializer<T> serializer, final long limit) {
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
          if (currentInputStream == null) {
            if (inputStreams.hasNext()) {
              currentInputStream = buildInputStream(inputStreams.next(), serializer.getChainables());
            } else {
              cannotContinueDecoding = true;
              return false;
            }
          }
          next = serializer.getCoder().decode(currentInputStream);
          hasNext = true;
          elementsDecoded++;
          return true;
        } catch (final IOException e) {
          currentInputStream = null;
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
  }

  /**
   * Wrap {@link InputStream} with {@link Chainable}s.
   *
   * @param in         the {@link InputStream}.
   * @param chainables the list of {@link Chainable} to be applied on the stream.
   * @return chained {@link InputStream}.
   * @throws IOException if fail to create new stream.
   */
  public static InputStream buildInputStream(final InputStream in, final List<Chainable> chainables)
  throws IOException {
    InputStream chained = in;
    for (final Chainable chainable : chainables) {
      chained = chainable.chainInput(chained);
    }
    return chained;
  }

  /**
   * Wrap {@link OutputStream} with {@link Chainable}s.
   *
   * @param out        the {@link OutputStream}.
   * @param chainables the list of {@link Chainable} to be applied on the stream.
   * @return chained {@link OutputStream}.
   * @throws IOException if fail to create new stream.
   */
  public static OutputStream buildOutputStream(final OutputStream out, final List<Chainable> chainables)
  throws IOException {
    OutputStream chained = out;
    final List<Chainable> temporaryChainableList = new ArrayList<>(chainables);
    Collections.reverse(temporaryChainableList);
    for (final Chainable chainable : temporaryChainableList) {
      chained = chainable.chainOutput(chained);
    }
    return chained;
  }
}
