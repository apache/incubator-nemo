package edu.snu.onyx.runtime.executor.data.filter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Filter {
  InputStream wrapInput(InputStream in) throws IOException;
  OutputStream wrapOutput(OutputStream out) throws IOException;
}
