package org.apache.nemo.client;

import org.apache.nemo.runtime.common.state.PlanState;

/**
 * A class provides the translation between the state of plan and corresponding {@link ClientEndpoint}.
 */
public interface StateTranslator {

  /**
   * Translate a plan state of nemo to a corresponding client endpoint state.
   *
   * @param planState to translate.
   * @return the translated state.
   */
  Enum translateState(final PlanState.State planState);
}
