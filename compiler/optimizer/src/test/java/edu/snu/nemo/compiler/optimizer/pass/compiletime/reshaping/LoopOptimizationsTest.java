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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import java.util.function.IntPredicate;

import org.junit.Test;

import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopOptimizations.LoopFusionPass;
import static org.junit.Assert.assertEquals;

public class LoopOptimizationsTest {

    @Test
    public void testcheckEqualityOfIntPredicates() {

        IntPredicate secondPredicate = number -> number < 10;
        IntPredicate firstPredicate = number -> number < 5;
        assertEquals(true,
                LoopFusionPass.checkEqualityOfIntPredicates(firstPredicate, secondPredicate, 4));
        assertEquals(false,
                LoopFusionPass.checkEqualityOfIntPredicates(firstPredicate, secondPredicate, 5));
        assertEquals(false,
                LoopFusionPass.checkEqualityOfIntPredicates(firstPredicate, secondPredicate, 7));
    }
}

    