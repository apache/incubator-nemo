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
package org.apache.nemo.runtime.executor.data;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;

public class SizeEstimatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(SizeEstimatorTest.class.getName());

  @Before
  public void setUp() {
    SizeEstimator.initialize();
  }


  @Test
  public void testArraySizeEstimation() {
    int[] arr = {1,2,3,4,5,6};
    assertEquals(48, SizeEstimator.estimate(arr));
  }

  @Test
  public void testClassSizeEstimation() {
    class TestSizeClass {
      int integer;
      long longInt;
      char character;
      byte singleByte;
      TestSizeClass(int integer, long longInt, char character, byte singleByte) {
        integer = integer;
        longInt = longInt;
        character = character;
        singleByte = singleByte;
      }
    }
    int integer = 1;
    long longInt = 1L;
    char character = 'a';
    byte singleByte = 1;
    TestSizeClass testClass = new TestSizeClass(integer, longInt, character, singleByte);
    // should equal 23 = 8 (object) + 4 (int) + 8 (long) + 2 (character) + 1 (byte)
    // add references and superclass info and align to get 40
    assertEquals(40, SizeEstimator.estimate(testClass));
  }

  @Test
  public void testPrimitiveClassSizeEstimation()   {
    Character singleChar = 'a';
    Byte singleByte = 1;
    Short shortInt = 1;
    Integer integer = 1;
    Long longInt = 1L;
    Float floatingPoint = 0.5f;
    Double doublePrecision = 0.5;
    // should all equal 16 despite difference in size due to class overhead + alignment issue
    assertEquals(16, SizeEstimator.estimate(singleChar));
    assertEquals(16, SizeEstimator.estimate(singleByte));
    assertEquals(16, SizeEstimator.estimate(shortInt));
    assertEquals(16, SizeEstimator.estimate(shortInt));
    assertEquals(16, SizeEstimator.estimate(integer));
    assertEquals(16, SizeEstimator.estimate(longInt));
    assertEquals(16, SizeEstimator.estimate(floatingPoint));
    assertEquals(16, SizeEstimator.estimate(doublePrecision));
    assertEquals(16, SizeEstimator.estimate(integer));
  }
}
