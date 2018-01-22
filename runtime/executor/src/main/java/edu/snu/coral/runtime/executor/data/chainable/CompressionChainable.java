package edu.snu.coral.runtime.executor.data.chainable;

import edu.snu.coral.common.exception.UnsupportedCompressionException;
import edu.snu.coral.common.ir.edge.executionproperty.CompressionProperty;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * {@link Chainable} for applying compression.
 */
public class CompressionChainable implements Chainable {
  private final CompressionProperty.Compression compression;

  /**
   * Constructor.
   *
   * @param compression compression method.
   */
  public CompressionChainable(final CompressionProperty.Compression compression) {
    this.compression = compression;
  }

  @Override
  public final InputStream chainInput(final InputStream in) throws IOException {
    switch (compression) {
      case Gzip:
        return new GZIPInputStream(in);
      case LZ4:
        return new LZ4BlockInputStream(in);
      default:
        throw new UnsupportedCompressionException("Not supported compression method");
    }
  }

  @Override
  public final OutputStream chainOutput(final OutputStream out) throws IOException {
    switch (compression) {
      case Gzip:
        return new GZIPOutputStream(out);
      case LZ4:
        return new LZ4BlockOutputStream(out);
      default:
        throw new UnsupportedCompressionException("Not supported compression method");
    }
  }
}
