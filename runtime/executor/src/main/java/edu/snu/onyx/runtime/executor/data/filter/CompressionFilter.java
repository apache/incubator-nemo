package edu.snu.onyx.runtime.executor.data.filter;

import edu.snu.onyx.common.exception.UnsupportedCompressionException;
import edu.snu.onyx.common.ir.edge.executionproperty.CompressionProperty;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionFilter implements Filter {
  private final CompressionProperty.Compression compression;

  public CompressionFilter(CompressionProperty.Compression compression) {
    this.compression = compression;
  }

  @Override
  public InputStream wrapInput(final InputStream in) throws IOException {
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
  public OutputStream wrapOutput(final OutputStream out) throws IOException {
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
