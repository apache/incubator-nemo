package org.apache.nemo.examples.beam;


import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.*;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;

public interface WordCountS3Option extends PipelineOptions, S3Options {
  @Description("AWS Access Key")
  @Validation.Required
  ValueProvider<String> getAWSAccessKey();
  void setAWSAccessKey(ValueProvider<String> value);

  @Description("AWS secret key")
  @Validation.Required
  ValueProvider<String> getAWSSecretKey();
  void setAWSSecretKey(ValueProvider<String> value);
}

