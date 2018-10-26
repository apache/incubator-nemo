package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Transform which saves elements to a HDFS text file for Spark.
 * @param <I> input type.
 */
public final class HDFSTextFileTransform<I> implements Transform<I, String> {
  private final String path;
  private Path fileName;
  private List<I> elements;

  /**
   * Constructor.
   *
   * @param path the path to write elements.
   */
  public HDFSTextFileTransform(final String path) {
    this.path = path;
  }

  @Override
  public void prepare(final Transform.Context context, final OutputCollector<String> outputCollector) {
    fileName = new Path(path + UUID.randomUUID().toString());
    this.elements = new ArrayList<>();
  }

  @Override
  public void onData(final I element) {
    elements.add(element);
  }

  @Override
  public void close() {
    try (
        final FileSystem fileSystem = fileName.getFileSystem(new JobConf());
        final FSDataOutputStream outputStream = fileSystem.create(fileName, false);
    ) {
      for (final I element : elements) {
        outputStream.writeBytes(element + "\n");
      }
      outputStream.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
