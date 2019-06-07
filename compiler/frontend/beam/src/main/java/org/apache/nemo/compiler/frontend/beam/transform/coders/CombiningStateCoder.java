package org.apache.nemo.compiler.frontend.beam.transform.coders;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalTransform;
import org.apache.nemo.compiler.frontend.beam.transform.InMemoryStateInternals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public final class CombiningStateCoder<InputT, AccumT, OutputT> extends Coder<CombiningState<InputT, AccumT, OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(CombiningStateCoder.class.getName());
  private final Coder<AccumT> coder;
  private final Combine.CombineFn<InputT, AccumT, OutputT> combineFn;

  public CombiningStateCoder(final Coder<AccumT> coder,
                             final Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
    this.coder = coder;
    this.combineFn = combineFn;
  }

  @Override
  public void encode(CombiningState<InputT, AccumT, OutputT> value, OutputStream outStream) throws CoderException, IOException {
    final AccumT state = value.getAccum();
    //LOG.info("Combining state: {}", state);

    coder.encode(state, outStream);
  }

  @Override
  public CombiningState<InputT, AccumT, OutputT> decode(InputStream inStream) throws CoderException, IOException {
    final AccumT accum = coder.decode(inStream);
    final CombiningState<InputT, AccumT, OutputT> state =
      new InMemoryStateInternals.InMemoryCombiningState<>(combineFn, coder);

    state.addAccum(accum);
    return state;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return coder.getCoderArguments();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    coder.verifyDeterministic();
  }
}
