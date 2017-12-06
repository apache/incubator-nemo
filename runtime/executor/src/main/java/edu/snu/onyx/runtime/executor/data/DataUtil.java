package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.DirectByteArrayOutputStream;
import edu.snu.onyx.common.coder.Coder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for data handling (e.g., (de)serialization).
 */
public final class DataUtil {
  private DataUtil() {
    // Private constructor.
  }

  /**
   * Serializes the elements in a non-serialized block into an output stream.
   *
   * @param coder              the coder to encode the elements.
   * @param nonSerializedBlock the non-serialized block to serialize.
   * @param bytesOutputStream  the output stream to write.
   * @return total number of elements in the block.
   * @throws IOException if fail to serialize.
   */
  public static long serializeBlock(final Coder coder,
                                    final NonSerializedBlock nonSerializedBlock,
                                    final ByteArrayOutputStream bytesOutputStream) throws IOException {
    long elementsCount = 0;
    for (final Object element : nonSerializedBlock.getData()) {
      coder.encode(element, bytesOutputStream);
      elementsCount++;
    }

    return elementsCount;
  }

  /**
   * Reads the data of a block from an input stream and deserializes it.
   *
   * @param elementsInBlock the number of elements in this block.
   * @param coder           the coder to decode the bytes.
   * @param hashValue       the hash value of the result block.
   * @param inputStream     the input stream which will return the data in the block as bytes.
   * @return the list of deserialized elements.
   * @throws IOException if fail to deserialize.
   */
  public static NonSerializedBlock deserializeBlock(final long elementsInBlock,
                                                    final Coder coder,
                                                    final int hashValue,
                                                    final InputStream inputStream) throws IOException {
    final List deserializedData = new ArrayList();
    for (int i = 0; i < elementsInBlock; i++) {
      deserializedData.add(coder.decode(inputStream));
    }
    return new NonSerializedBlock(hashValue, deserializedData);
  }

  /**
   * Converts the non-serialized {@link Block}s in an iterable to serialized {@link Block}s.
   *
   * @param coder           the coder for serialization.
   * @param blocksToConvert the blocks to convert.
   * @return the converted {@link SerializedBlock}s.
   * @throws IOException if fail to convert.
   */
  public static Iterable<SerializedBlock> convertToSerBlocks(final Coder coder,
                                                             final Iterable<NonSerializedBlock> blocksToConvert)
      throws IOException {
    final List<SerializedBlock> serializedBlocks = new ArrayList<>();
    for (final NonSerializedBlock blockToConvert : blocksToConvert) {
      try (final DirectByteArrayOutputStream bytesOutputStream = new DirectByteArrayOutputStream()) {
        final long elementsTotal = serializeBlock(coder, blockToConvert, bytesOutputStream);
        final byte[] serializedBytes = bytesOutputStream.getBufDirectly();
        final int actualLength = bytesOutputStream.getCount();
        serializedBlocks.add(
            new SerializedBlock(blockToConvert.getKey(), elementsTotal, serializedBytes, actualLength));
      }
    }
    return serializedBlocks;
  }

  /**
   * Converts the serialized {@link Block}s in an iterable to non-serialized {@link Block}s.
   *
   * @param coder           the coder for deserialization.
   * @param blocksToConvert the blocks to convert.
   * @return the converted {@link NonSerializedBlock}s.
   * @throws IOException if fail to convert.
   */
  public static Iterable<NonSerializedBlock> convertToNonSerBlocks(final Coder coder,
                                                                   final Iterable<SerializedBlock> blocksToConvert)
      throws IOException {
    final List<NonSerializedBlock> nonSerializedBlocks = new ArrayList<>();
    for (final SerializedBlock blockToConvert : blocksToConvert) {
      final int hashVal = blockToConvert.getKey();
      try (final ByteArrayInputStream byteArrayInputStream =
               new ByteArrayInputStream(blockToConvert.getData())) {
        final NonSerializedBlock deserializeBlock = deserializeBlock(
            blockToConvert.getElementsTotal(), coder, hashVal, byteArrayInputStream);
        nonSerializedBlocks.add(deserializeBlock);
      }
    }
    return nonSerializedBlocks;
  }

  /**
   * Converts a partition id to the corresponding file path.
   *
   * @param partitionId   the ID of the partition.
   * @param fileDirectory the directory of the target partition file.
   * @return the file path of the partition.
   */
  public static String partitionIdToFilePath(final String partitionId,
                                             final String fileDirectory) {
    return fileDirectory + "/" + partitionId;
  }

  /**
   * Concatenates an iterable of non-serialized {@link Block}s into a single iterable of elements.
   *
   * @param blocksToConcat the blocks to concatenate.
   * @return the concatenated iterable of all elements.
   * @throws IOException if fail to concatenate.
   */
  public static Iterable concatNonSerBlocks(final Iterable<NonSerializedBlock> blocksToConcat) throws IOException {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (final NonSerializedBlock nonSerializedBlock : blocksToConcat) {
      final Iterable elementsInBlock = nonSerializedBlock.getData();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(elementsInBlock.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
