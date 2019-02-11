package org.apache.nemo.common;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;

import java.io.Serializable;
import java.util.Optional;

public interface OffloadingSerializer<I, O> extends Serializable {

  EncoderFactory<I> getInputEncoder();

  DecoderFactory<I> getInputDecoder();

  EncoderFactory<O> getOutputEncoder();

  DecoderFactory<O> getOutputDecoder();
}
