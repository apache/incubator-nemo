package org.apache.nemo.compiler.frontend.beam.coder;

import org.apache.beam.sdk.coders.AtomicCoder;

import java.io.*;

/**
 * EncoderFactory for float[].
 */
public final class FloatArrayCoder extends AtomicCoder<float[]> {
  /**
   * Private constructor.
   */
  private FloatArrayCoder() {
  }

  /**
   * @return a new coder
   */
  public static FloatArrayCoder of() {
    return new FloatArrayCoder();
  }

  @Override
  public void encode(final float[] ary, final OutputStream outStream) throws IOException {
    final DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeInt(ary.length);
    for (float f : ary) {
      dataOutputStream.writeFloat(f);
    }
  }

  @Override
  public float[] decode(final InputStream inStream) throws IOException {
    final DataInputStream dataInputStream = new DataInputStream(inStream);
    final int floatArrayLen = dataInputStream.readInt();
    final float[] floatArray = new float[floatArrayLen];
    for (int i = 0; i < floatArrayLen; i++) {
      floatArray[i] = dataInputStream.readFloat();
    }
    return floatArray;
  }
}
