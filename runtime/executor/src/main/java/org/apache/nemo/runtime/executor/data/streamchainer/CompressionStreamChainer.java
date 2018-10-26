package org.apache.nemo.runtime.executor.data.streamchainer;

import org.apache.nemo.common.exception.UnsupportedCompressionException;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * {@link EncodeStreamChainer} for applying compression.
 */
public class CompressionStreamChainer implements EncodeStreamChainer {
  private final CompressionProperty.Value compression;

  /**
   * Constructor.
   *
   * @param compression compression method.
   */
  public CompressionStreamChainer(final CompressionProperty.Value compression) {
    this.compression = compression;
  }

  @Override
  public final OutputStream chainOutput(final OutputStream out) throws IOException {
    switch (compression) {
      case Gzip:
        return new GZIPOutputStream(out);
      case LZ4:
        return new LZ4BlockOutputStream(out);
      case None:
        return out;
      default:
        throw new UnsupportedCompressionException("Not supported compression method");
    }
  }
}
