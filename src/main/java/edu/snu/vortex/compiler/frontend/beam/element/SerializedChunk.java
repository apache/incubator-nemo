package edu.snu.vortex.compiler.frontend.beam.element;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.ValueInSingleWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SerializedChunk<T> extends Element<T> {
  private static final Coder coder =
      WindowedValue.getFullCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()), IntervalWindow.getCoder());

  private final List<byte[]> serList;

  public SerializedChunk() {
    this.serList = new ArrayList<>();
  }

  public void addWinVal(final WindowedValue wv) {
    try {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();
      SerializedChunk.coder.encode(wv, bos, Coder.Context.NESTED);
      serList.add(bos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isEmpty() {
    return serList.isEmpty();
  }

  public List<WindowedValue> getWinVals() {
    try {
      final List<WindowedValue> result = new ArrayList<>();
      for (final byte[] ser : serList) {
        result.add((WindowedValue)SerializedChunk.coder.decode(new ByteArrayInputStream(ser), Coder.Context.NESTED));
      }
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "SerializedChunk";
  }
}
