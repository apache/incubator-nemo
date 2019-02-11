package org.apache.nemo.common;

import java.io.*;
import java.util.Base64;

/**
 * A utility class for serialization and deserialization of objects.
 */
public final class SerializeUtils {

  private SerializeUtils() {
    // do nothing
  }

  /**
   * Read the object from Base64 string.
   * @param s serialized object
   * @param <T> object type
   * @return object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static <T> T deserializeFromString(
      final String s) {
    try {
      final byte[] data = Base64.getDecoder().decode(s);
      final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
      final T object = (T) ois.readObject();
      ois.close();
      return object;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Read the object from Base64 string with the external class loader.
   * @param classLoader an external class loader
   * @param <T> object type
   * @return object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static <T> T deserialize(
      final InputStream in,
      final ClassLoader classLoader) {
    try {
      final ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
        classLoader, in);
      final T object = (T) stream.readObject();
      //stream.close();
      return object;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Read the object from Base64 string with the external class loader.
   * @param s serialized object
   * @param classLoader an external class loader
   * @param <T> object type
   * @return object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static <T> T deserializeFromString(
      final String s,
      final ClassLoader classLoader) {
    try {
      final byte[] data = Base64.getDecoder().decode(s);
      final ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
        classLoader, data);
      final T object = (T) stream.readObject();
      stream.close();
      return object;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static <T> T deserialize(
    final byte[] bytes,
    final ClassLoader classLoader) {
    try {
      final ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
        classLoader, bytes);
      final T object = (T) stream.readObject();
      stream.close();
      return object;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Write the object to a Base64 string.
   * @param obj object
   * @return serialized object
   * @throws IOException
   */
  public static String serializeToString(final Serializable obj) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
