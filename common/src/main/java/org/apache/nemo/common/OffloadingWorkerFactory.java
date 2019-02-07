package org.apache.nemo.common;

import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

public interface OffloadingWorkerFactory {

  OffloadingWorker createOffloadingWorker(List<String> serializedTransforms,
                                          EncoderFactory inputEncoderFactory,
                                          DecoderFactory inputDecoderFactory,
                                          EncoderFactory outputEncoderFactory,
                                          DecoderFactory outputDecoderFactory);
}
