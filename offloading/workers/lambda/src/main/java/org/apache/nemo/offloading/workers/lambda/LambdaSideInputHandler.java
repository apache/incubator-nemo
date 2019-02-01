package org.apache.nemo.offloading.workers.lambda;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.OutputCollector;

import java.io.Serializable;

public interface LambdaSideInputHandler<M, S, R> extends Serializable {

  void processMainAndSideInput(M mainInput, S sideInput, OutputCollector<R> collector);
}
