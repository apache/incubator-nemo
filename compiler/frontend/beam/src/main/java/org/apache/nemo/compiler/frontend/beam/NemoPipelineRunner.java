package org.apache.nemo.compiler.frontend.beam;

import org.apache.nemo.client.JobLauncher;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * Runner class for BEAM programs.
 */
public final class NemoPipelineRunner extends PipelineRunner<NemoPipelineResult> {
  private final NemoPipelineOptions nemoPipelineOptions;

  /**
   * BEAM Pipeline Runner.
   * @param nemoPipelineOptions PipelineOptions.
   */
  private NemoPipelineRunner(final NemoPipelineOptions nemoPipelineOptions) {
    this.nemoPipelineOptions = nemoPipelineOptions;
  }

  /**
   * Static initializer for creating PipelineRunner with the given options.
   * @param options given PipelineOptions.
   * @return The created PipelineRunner.
   */
  public static PipelineRunner<NemoPipelineResult> fromOptions(final PipelineOptions options) {
    final NemoPipelineOptions nemoOptions = PipelineOptionsValidator.validate(NemoPipelineOptions.class, options);
    return new NemoPipelineRunner(nemoOptions);
  }

  /**
   * Method to run the Pipeline.
   * @param pipeline the Pipeline to run.
   * @return The result of the pipeline.
   */
  public NemoPipelineResult run(final Pipeline pipeline) {
    final PipelineVisitor pipelineVisitor = new PipelineVisitor();
    pipeline.traverseTopologically(pipelineVisitor);
    final DAG<IRVertex, IREdge> dag = PipelineTranslator.translate(pipeline,
      pipelineVisitor.getConvertedPipeline(),
      nemoPipelineOptions);

    final NemoPipelineResult nemoPipelineResult = new NemoPipelineResult();
    JobLauncher.launchDAG(dag);
    return nemoPipelineResult;
  }
}
