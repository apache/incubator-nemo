package org.apache.nemo.compiler.frontend.beam;

import org.apache.nemo.client.ClientEndpoint;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * Beam result.
 */
public final class NemoPipelineResult extends ClientEndpoint implements PipelineResult {

  /**
   * Default constructor.
   */
  public NemoPipelineResult() {
    super(new BeamStateTranslator());
  }

  @Override
  public State getState() {
    return (State) super.getPlanState();
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("cancel() in frontend.beam.NemoPipelineResult");
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    throw new UnsupportedOperationException();
    // TODO #208: NemoPipelineResult#waitUntilFinish hangs
    // Previous code that hangs the job:
    // return (State) super.waitUntilJobFinish(duration.getMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public State waitUntilFinish() {
    throw new UnsupportedOperationException();
    // TODO #208: NemoPipelineResult#waitUntilFinish hangs
    // Previous code that hangs the job:
    // return (State) super.waitUntilJobFinish();
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("metrics() in frontend.beam.NemoPipelineResult");
  }
}
