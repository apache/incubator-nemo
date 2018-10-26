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

/**
 * This interface is implemented by objects that can be cloned.
 * <p>
 * This interface also overcomes the drawback of {@link java.lang.Cloneable} interface which
 * doesn't have a clone method.
 * Josh Bloch (a JDK author) has pointed out this in his book "Effective Java" as
 * <a href="http://thefinestartist.com/effective-java/11"> Override clone judiciously </a>
 * </p>
 *
 * @param <T> the type of objects that this class can clone
 */
public interface Cloneable<T extends Cloneable<T>> {

  /**
   * Creates and returns a copy of this object.
   * <p>
   * The precise meaning of "copy" may depend on the class of the object.
   * The general intent is that, all fields of the object are copied.
   * </p>
   *
   * @return a clone of this object.
   */
  T getClone();
}
