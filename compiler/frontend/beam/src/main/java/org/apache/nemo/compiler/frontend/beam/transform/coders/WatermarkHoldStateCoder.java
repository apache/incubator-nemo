package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.joda.time.Instant;
import org.nustaq.serialization.FSTConfiguration;

import java.io.*;
import java.util.Collections;
import java.util.List;

public final class WatermarkHoldStateCoder extends Coder<WatermarkHoldState> {


  private WatermarkHoldStateCoder() {
  }

  private static final WatermarkHoldStateCoder INSTANCE = new WatermarkHoldStateCoder();

  public static WatermarkHoldStateCoder getInstance() {
    return INSTANCE;
  }

  @Override
  public void encode(WatermarkHoldState value, OutputStream outStream) throws CoderException, IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();
    final DataOutputStream dos = new DataOutputStream(outStream);

    final Instant instant = value.read();

    if (instant == null) {
      outStream.write(1);
    } else {
      outStream.write(0);

      dos.writeLong(instant.getMillis());
    }

    final TimestampCombiner timestampCombiner = value.getTimestampCombiner();
    conf.encodeToStream(outStream, timestampCombiner);
  }

  @Override
  public WatermarkHoldState decode(InputStream inStream) throws CoderException, IOException {
    final FSTConfiguration conf = FSTSingleton.getInstance();
    final DataInputStream dis = new DataInputStream(inStream);

    final int b = inStream.read();

    final Instant instant;

    try {
      if (b == 0) {
        instant = new Instant(dis.readLong());
      } else {
        instant = null;
      }

      final TimestampCombiner timestampCombiner = (TimestampCombiner) conf.decodeFromStream(inStream);
      final WatermarkHoldState watermarkHoldState = new InMemoryStateInternals.InMemoryWatermarkHold<>(timestampCombiner);

      if (instant != null) {
        watermarkHoldState.add(instant);
      }
      return watermarkHoldState;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
  }
}
