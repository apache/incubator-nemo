package org.apache.nemo.offloading.common;

import java.io.*;

/**
 * This class is used for reading objects from external JARs.
 */
public final class ExternalJarObjectInputStream extends ObjectInputStream {

  private ClassLoader classLoader;

  public ExternalJarObjectInputStream(final ClassLoader classLoader, final byte[] bytes) throws IOException {
    super(new ByteArrayInputStream(bytes));
    this.classLoader = classLoader;
  }

  public ExternalJarObjectInputStream(final ClassLoader classLoader, final InputStream in) throws IOException {
    super(in);
    this.classLoader = classLoader;
  }

  @Override
  protected Class<?> resolveClass(final ObjectStreamClass desc) throws ClassNotFoundException {
    String name = desc.getName();
    try {
      return Class.forName(name, false, classLoader);
    } catch (ClassNotFoundException ex) {
      throw ex;
    }
  }
}
