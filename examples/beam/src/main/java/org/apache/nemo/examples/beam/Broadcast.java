package org.apache.nemo.examples.beam;

import org.apache.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Optional;
import java.util.stream.StreamSupport;

/**
 * Sample Broadcast application.
 */
public final class Broadcast {
  /**
   * Private constructor.
   */
  private Broadcast() {
  }

  /**
   * Main function for the BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(NemoPipelineRunner.class);

    final Pipeline p = Pipeline.create(options);
    final PCollection<String> elemCollection = GenericSourceSink.read(p, inputFilePath);
    final PCollectionView<Iterable<String>> allCollection = elemCollection.apply(View.<String>asIterable());

    final PCollection<String> result = elemCollection.apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(final ProcessContext c) {
            final String line = c.element();
            final Iterable<String> all = c.sideInput(allCollection);
            final Optional<String> appended = StreamSupport.stream(all.spliterator(), false)
                .reduce((l, r) -> l + '\n' + r);
            if (appended.isPresent()) {
              c.output("line: " + line + "\n" + appended.get());
            } else {
              c.output("error");
            }
          }
        }).withSideInputs(allCollection)
    );

    GenericSourceSink.write(result, outputFilePath);
    p.run();
  }
}
