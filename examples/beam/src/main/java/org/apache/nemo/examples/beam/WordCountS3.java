package org.apache.nemo.examples.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountS3 {

  private static final Logger log = LoggerFactory.getLogger(WordCountS3.class);

  public static void main(String[] args) {
    final PipelineOptions options = NemoPipelineOptionsFactory.create();
    options.setJobName("WordCountS3");
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> fileLines =
      pipeline.apply("ReadFromFile", TextIO.read().from("s3://lambda-executor-examples/sample_1.csv"));

    fileLines.apply("PrintLines", MapElements.via(new SimpleFunction<String, Void>() {
      @Override
      public Void apply(String lines) {
        System.out.println(lines);
        return null;
      }
    }));

    PipelineResult result = pipeline.run();
    try {
      result.getState(); // To skip the error while creating the template
      result.waitUntilFinish();
    } catch (UnsupportedOperationException e) {
      log.error("UnsupportedOperationException :" + e.getMessage());
    } catch (Exception e) {
      log.error("Exception :" + e.getMessage(), e);
    }
  }
}

