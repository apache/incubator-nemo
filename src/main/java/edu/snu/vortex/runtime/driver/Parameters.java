package edu.snu.vortex.runtime.driver;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

public final class Parameters {

  public static final String NCS_ID = "VORTEX_NCS";

  @NamedParameter(short_name = "runtime", default_value = "local")
  public static final class Runtime implements Name<String> {
  }

  @NamedParameter(short_name = "eval_num", default_value = "2")
  public static final class EvaluatorNum implements Name<Integer> {
  }

  @NamedParameter(short_name = "eval_mem", default_value = "1024")
  public static final class EvaluatorMem implements Name<Integer> {
  }

  @NamedParameter(short_name = "eval_core", default_value = "1")
  public static final class EvaluatorCore implements Name<Integer> {
  }

  @NamedParameter(short_name = "args", default_value = "")
  public static final class UserArguments implements Name<String> {
  }
}
