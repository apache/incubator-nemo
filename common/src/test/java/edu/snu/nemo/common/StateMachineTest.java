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

import edu.snu.nemo.common.StateMachine;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link StateMachine}
 */
public final class StateMachineTest {
  private StateMachine.Builder stateMachineBuilder;

  @Before
  public void setUp() {
    this.stateMachineBuilder = StateMachine.newBuilder();
  }

  @Test
  public void testSimpleStateTransitions() {
    stateMachineBuilder.addState(CookingState.SHOPPING, "Shopping for ingredients");
    stateMachineBuilder.addState(CookingState.PREPARING, "Washing vegetables, chopping meat...");
    stateMachineBuilder.addState(CookingState.SEASONING, "Adding salt and pepper");
    stateMachineBuilder.addState(CookingState.COOKING, "The food is in the oven");
    stateMachineBuilder.addState(CookingState.READY_TO_EAT, "Let's eat");

    stateMachineBuilder.addTransition(CookingState.SHOPPING, CookingState.PREPARING, "");
    stateMachineBuilder.addTransition(CookingState.PREPARING, CookingState.SEASONING, "");
    stateMachineBuilder.addTransition(CookingState.SEASONING, CookingState.COOKING, "");
    stateMachineBuilder.addTransition(CookingState.COOKING, CookingState.READY_TO_EAT, "");

    stateMachineBuilder.setInitialState(CookingState.SHOPPING);

    final StateMachine stateMachine = stateMachineBuilder.build();

    assertEquals(CookingState.SHOPPING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.SHOPPING, CookingState.PREPARING));

    assertEquals(CookingState.PREPARING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.PREPARING, CookingState.SEASONING));

    assertEquals(CookingState.SEASONING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.SEASONING, CookingState.COOKING));

    assertEquals(CookingState.COOKING, stateMachine.getCurrentState());
    assertTrue(stateMachine.compareAndSetState(CookingState.COOKING, CookingState.READY_TO_EAT));
  }

  private enum CookingState {
    SHOPPING,
    PREPARING,
    SEASONING,
    COOKING,
    READY_TO_EAT
  }
}
