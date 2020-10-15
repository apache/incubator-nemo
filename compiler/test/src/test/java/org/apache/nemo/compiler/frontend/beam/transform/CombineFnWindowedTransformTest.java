package org.apache.nemo.compiler.frontend.beam.transform;

import junit.framework.TestCase;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombineFnWindowedTransformTest extends TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(CombineFnWindowedTransformTest.class.getName());
  private final static Coder STRING_CODER = StringUtf8Coder.of();
  private final static Coder INTEGER_CODER = BigEndianIntegerCoder.of();
}
