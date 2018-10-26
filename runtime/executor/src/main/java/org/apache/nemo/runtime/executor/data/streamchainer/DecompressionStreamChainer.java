package org.apache.nemo.runtime.executor.data.streamchainer;

import org.apache.nemo.common.exception.UnsupportedCompressionException;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import net.jpountz.lz4.LZ4BlockInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * {@link DecodeStreamChainer} for applying compression.
 */
public class DecompressionStreamChainer implements DecodeStreamChainer {
  private final CompressionProperty.Value compression;

  /**
   * Constructor.
   *
   * @param compression compression method.
   */
  public DecompressionStreamChainer(final CompressionProperty.Value compression) {
    this.compression = compression;
  }

  @Override
  public final InputStream chainInput(final InputStream in) throws IOException {
    switch (compression) {
      case Gzip:
        return new GZIPInputStream(in);
      case LZ4:
        return new LZ4BlockInputStream(in);
      case None:
        return in;
      default:
        throw new UnsupportedCompressionException("Not supported compression method");
    }
  }
}
