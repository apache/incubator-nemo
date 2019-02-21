package org.apache.nemo.offloading.common;

import java.io.Serializable;

public interface OffloadingSerializer<I, O> extends Serializable {

  OffloadingEncoder<I> getInputEncoder();

  OffloadingDecoder<I> getInputDecoder();

  OffloadingEncoder<O> getOutputEncoder();

  OffloadingDecoder<O> getOutputDecoder();
}
