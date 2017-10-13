package edu.snu.onyx.runtime.common.message.ncs;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters for NCS.
 */
public final class NcsParameters {

  /**
   * Id of the sender.
   */
  @NamedParameter
  public static final class SenderId implements Name<String> {
  }
}
