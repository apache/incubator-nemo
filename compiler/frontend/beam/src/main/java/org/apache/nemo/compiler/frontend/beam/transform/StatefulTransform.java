package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.sdk.coders.Coder;

public interface StatefulTransform<S> {


  Coder<S> getStateCoder();
  S getState();
  void setState(S state);
}
