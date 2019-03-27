/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.function.IntPredicate;

import static junit.framework.TestCase.assertEquals;

public class UtilTest {
  @Test
  public void testRootPath() {
    final String one = Util.recursivelyFindLicense(Paths.get(System.getProperty("user.dir")));
    final String two = Util.recursivelyFindLicense(Paths.get(System.getProperty("user.dir")).getParent());
    Assert.assertEquals(one, two);
  }

  @Test
  public void testCheckEqualityOfIntPredicates() {

    IntPredicate firstPredicate = number -> number < 5;
    IntPredicate secondPredicate = number -> number < 10;
    assertEquals(true,
      Util.checkEqualityOfIntPredicates(firstPredicate, secondPredicate, 4));
    assertEquals(false,
      Util.checkEqualityOfIntPredicates(firstPredicate, secondPredicate, 5));
    assertEquals(false,
      Util.checkEqualityOfIntPredicates(firstPredicate, secondPredicate, 7));
  }
}
