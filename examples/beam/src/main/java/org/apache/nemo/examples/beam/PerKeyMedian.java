package org.apache.nemo.examples.beam;

import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IP stat analysis example used in skew experiment.
 */
public final class PerKeyMedian {
  private static final Logger LOG = LoggerFactory.getLogger(PerKeyMedian.class.getName());

  /**
   * Private Constructor.
   */
  private PerKeyMedian() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("PerKeyMedian");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
          @Override
          public KV<String, String> apply(final String line) {
            final String[] words = line.split(" ");
            String key = words[0];
            String value = words[1];
            return KV.of(key, value);
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, String>() {
          @Override
          public String apply(final KV<String, Iterable<String>> kv) {
            final String key = kv.getKey();
            List value = Lists.newArrayList(kv.getValue());
            Collections.sort(value);
            return key + ", " + value.get(value.size() / 2);
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }
}
