package edu.snu.vortex.runtime.driver;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

public final class Parameters {

  @NamedParameter
  public static final class EvaluatorNum implements Name<Integer> {
  }

  @NamedParameter
  public static final class EvaluatorMem implements Name<Integer> {
  }

  @NamedParameter
  public static final class EvaluatorCore implements Name<Integer> {
  }

  @NamedParameter
  public static final class UserArguments implements Name<String> {
  }
}
