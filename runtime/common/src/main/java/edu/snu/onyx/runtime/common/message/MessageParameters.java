package edu.snu.onyx.runtime.common.message;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters for message components.
 */
public class MessageParameters {

  /**
   * Id of the sender.
   */
  @NamedParameter
  public static final class SenderId implements Name<String> {
  }
}
