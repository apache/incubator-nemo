package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;

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
   * Serializes the elements in a block into an output stream.
   *
   * @param coder             the coder to encode the elements.
   * @param block             the block to serialize.
   * @param bytesOutputStream the output stream to write.
   * @return total number of elements in the block.
   * @throws IOException if fail to serialize.
   */
  public static long serializeBlock(final Coder coder,
                                    final Block block,
                                    final ByteArrayOutputStream bytesOutputStream) throws IOException {
    long elementsCount = 0;
    for (final Object element : block.getElements()) {
      coder.encode(element, bytesOutputStream);
      elementsCount++;
    }

    return elementsCount;
  }

  /**
   * Reads the data of a block from an input stream and deserializes it.
   *
   * @param elementsInBlock  the number of elements in this block.
   * @param coder            the coder to decode the bytes.
   * @param inputStream      the input stream which will return the data in the block as bytes.
   * @param deserializedData the list of elements to put the deserialized data.
   * @throws IOException if fail to deserialize.
   */
  public static void deserializeBlock(final long elementsInBlock,
                                      final Coder coder,
                                      final InputStream inputStream,
                                      final List deserializedData) throws IOException {
    for (int i = 0; i < elementsInBlock; i++) {
      deserializedData.add(coder.decode(inputStream));
    }
  }

  /**
   * Gets data coder from the {@link PartitionManagerWorker}.
   *
   * @param partitionId the ID of the partition to get the coder.
   * @param worker      the {@link PartitionManagerWorker} having coder.
   * @return the coder.
   */
  public static Coder getCoderFromWorker(final String partitionId,
                                         final PartitionManagerWorker worker) {
    final String runtimeEdgeId = RuntimeIdGenerator.getRuntimeEdgeIdFromPartitionId(partitionId);
    return worker.getCoder(runtimeEdgeId);
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
   * Concatenates an iterable of blocks into a single iterable of elements.
   *
   * @param blocksToConcat the blocks to concatenate.
   * @return the concatenated iterable of all elements.
   */
  public static Iterable concatBlocks(final Iterable<Block> blocksToConcat) {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (final Block block : blocksToConcat) {
      final Iterable elementsInBlock = block.getElements();
      concatStream = Stream.concat(concatStream, StreamSupport.stream(elementsInBlock.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
