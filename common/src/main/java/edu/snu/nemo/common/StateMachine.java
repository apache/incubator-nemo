/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.common;

import edu.snu.nemo.common.exception.IllegalStateTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A finite state machine that can be created with user defined states and transitions.
 */
// TODO #17: StateMachine Visualization
public final class StateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(StateMachine.class.getName());

  private final Map<Enum, State> stateMap;
  private State currentState;

  /**
   * Private constructor.
   * @param stateMap Map of state enum to the state.
   * @param initialState initial state of the state machine.
   */
  private StateMachine(final Map<Enum, State> stateMap, final Enum initialState) {
    this.stateMap = stateMap;
    this.currentState = stateMap.get(initialState);
  }

  /**
   * Checks whether the current state is same as the {@code expectedCurrentState}.
   *
   * @param expectedCurrentState the expected current state
   * @throws RuntimeException if the expectedCurrentState is not same as the actual current state
   */
  public synchronized void checkState(final Enum expectedCurrentState) {
    if (!currentState.stateEnum.equals(expectedCurrentState)) {
      final String exceptionMessage = new StringBuilder()
          .append("The expected current state is ")
          .append(expectedCurrentState)
          .append(" but the actual state is ")
          .append(currentState).append('\n')
          .append(getPossibleTransitionsFromCurrentState())
          .toString();

      throw new IllegalStateException(exceptionMessage);
    }
  }

  /**
   * Sets the current state as a certain state.
   *
   * @param state a state
   * @throws RuntimeException if the state is unknown state, or the transition
   * from the current state to the specified state is illegal
   */
  public synchronized void setState(final Enum state) {
    if (!stateMap.containsKey(state)) {
      throw new RuntimeException("Unknown state " + state);
    }

    final State toState = stateMap.get(state);
    if (!currentState.isLegalTransition(state)) {
      final String exceptionMessage = new StringBuilder()
          .append("Illegal transition from ")
          .append(currentState)
          .append(" to ")
          .append(toState).append('\n')
          .append(getPossibleTransitionsFromCurrentState())
          .toString();
      throw new IllegalStateTransitionException(new Exception(exceptionMessage));
    }

    currentState = toState;
  }

  /**
   * Atomically sets the state to the given updated state
   * if the current state equals to the expected state.
   *
   * @param expectedCurrentState an expected state
   * @param state a state
   * @return {@code true} if successful. {@code false} indicates that
   * the actual value was not equal to the expected value.
   * @throws RuntimeException if the state is unknown state, or the transition
   * from the current state to the specified state is illegal
   */
  public synchronized boolean compareAndSetState(final Enum expectedCurrentState, final Enum state) {
    final boolean compared = currentState.stateEnum.equals(expectedCurrentState);
    if (compared) {
      setState(state);
    } else {
      LOG.debug("The expected current state [" + expectedCurrentState
          + "] is different from the actual state [" + currentState.stateEnum + "]");
    }

    return compared;
  }

  /**
   * @return the name of the current state.
   */
  public synchronized Enum getCurrentState() {
    return currentState.stateEnum;
  }

  /**
   * @return String of possible transitions from the current state.
   */
  private String getPossibleTransitionsFromCurrentState() {
    final StringBuilder stringBuilder = new StringBuilder()
        .append("Possible transitions from the current state are").append('\n');

    for (final Transition transition : currentState.getAllTransitions()) {
      stringBuilder.append(transition).append('\n');
    }

    return stringBuilder.toString();
  }

  @Override
  public String toString() {
    final StringBuilder stringBuilder = new StringBuilder();
    for (final State state : stateMap.values()) {
      stringBuilder.append(state).append('\n')
          .append("Possible transitions:").append('\n');
      for (final Transition transition : state.getAllTransitions()) {
        stringBuilder.append(transition).append('\n');
      }
      stringBuilder.append('\n');
    }
    return stringBuilder.toString();
  }

  /**
   * @return a builder of StateMachine
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Keeps track of states and transitions for the state machine.
   */
  private static final class State {
    private final Enum stateEnum;
    private final String description;
    private final Map<Enum, Transition> transitions;

    /**
     * Private constructor.
     * @param stateEnum Enum of the state.
     * @param description Description of the state.
     */
    private State(final Enum stateEnum, final String description) {
      this.stateEnum = stateEnum;
      this.description = description;
      this.transitions = new HashMap<>();
    }

    /**
     * Adds a transition to the state.
     * @param transition the added transition.
     */
    private void addTransition(final Transition transition) {
      if (transition.from != this) {
        throw new RuntimeException("An illegal transition " + transition + " was added to " + this);
      }

      transitions.put(transition.to.stateEnum, transition);
    }

    /**
     * @param to transition that we try switching to.
     * @return whether or not this transition is legal.
     */
    private boolean isLegalTransition(final Enum to) {
      return transitions.containsKey(to);
    }

    /**
     * @return all transitions.
     */
    private Collection<Transition> getAllTransitions() {
      return transitions.values();
    }

    @Override
    public String toString() {
      return stateEnum + "[" + description + "]";
    }
  }

  /**
   * Represents a change from one state to another.
   */
  private static final class Transition {
    private final State from;
    private final State to;
    private final String description;

    /**
     * Private constructor.
     * @param from State from which we switch.
     * @param to State that we switch to.
     * @param description Description of the transition.
     */
    private Transition(final State from, final State to, final String description) {
      this.from = from;
      this.to = to;
      this.description = description;
    }

    @Override
    public String toString() {
      return "Transition from " + from + " to " + to + " : " + description;
    }
  }

  /**
   * Builder that builds a StateMachine.
   */
  public static final class Builder {
    private final Set<Enum> stateEnumSet;
    private final Map<Enum, String> stateDescriptionMap;
    private final Map<Enum, Set<Pair<Enum, String>>> transitionMap;

    private Enum initialState;

    /**
     * Private constructor.
     */
    private Builder() {
      this.stateEnumSet = new HashSet<>();
      this.stateDescriptionMap = new HashMap<>();
      this.transitionMap = new HashMap<>();
    }

    /**
     * Adds a state with name and description.
     *
     * @param stateEnum enumeration indicating the state
     * @param description description of the state
     * @return the builder
     * @throws RuntimeException if the state was already added
     */
    public Builder addState(final Enum stateEnum, final String description) {
      if (stateEnumSet.contains(stateEnum)) {
        throw new RuntimeException("A state " + stateEnum + " was already added");
      }

      stateEnumSet.add(stateEnum);
      stateDescriptionMap.put(stateEnum, description);
      return this;
    }

    /**
     * @param stateToSet the initial state for StateMachine
     * @return the builder
     * @throws RuntimeException if the initial state was not added first
     */
    public Builder setInitialState(final Enum stateToSet) {
      if (!stateEnumSet.contains(stateToSet)) {
        throw new RuntimeException("A state " + stateToSet + " should be added first");
      }
      this.initialState = stateToSet;
      return this;
    }

    /**
     * Adds a transition with description.
     *
     * @param from from state name
     * @param to to state name
     * @param description description of the transition
     * @return the builder
     * @throws RuntimeException if either from or to state was not added, or the same transition
     * was already added
     */
    public Builder addTransition(final Enum from, final Enum to, final String description) {
      if (!stateEnumSet.contains(from)) {
        throw new RuntimeException("A state " + from + " should be added first");
      }

      if (!stateEnumSet.contains(to)) {
        throw new RuntimeException("A state " + to + " should be added first");
      }

      final Pair<Enum, String> transition = Pair.of(to, description);

      if (!transitionMap.containsKey(from)) {
        transitionMap.put(from, new HashSet<>());
      }

      if (transitionMap.get(from).contains(transition)) {
        throw new RuntimeException("A transition from " + from + " to " + to + " was already added");
      }

      transitionMap.get(from).add(transition);
      return this;
    }

    /**
     * Builds and returns the StateMachine.
     *
     * @return the StateMachine
     * @throws RuntimeException if an initial state was not set
     */
    public StateMachine build() {
      if (initialState == null) {
        throw new RuntimeException("An initial state should be set");
      }

      final Map<Enum, State> stateMap = new HashMap<>();
      for (final Enum stateEnum : stateEnumSet) {
        stateMap.put(stateEnum, new State(stateEnum, stateDescriptionMap.get(stateEnum)));
      }

      for (final Enum stateEnum : stateEnumSet) {
        final State state = stateMap.get(stateEnum);
        if (transitionMap.containsKey(stateEnum)) {
          for (final Pair<Enum, String> transition : transitionMap.get(stateEnum)) {
            state.addTransition(new Transition(state, stateMap.get(transition.left()), transition.right()));
          }
        }
      }

      return new StateMachine(stateMap, initialState);
    }
  }
}
