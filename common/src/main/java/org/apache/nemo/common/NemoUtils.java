package org.apache.nemo.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

public final class NemoUtils {

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
