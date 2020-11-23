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
package org.apache.reef.runtime.yarn;

import org.apache.reef.util.HadoopEnvironment;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * A helper class to assemble a class path.
 * <p>
 * It uses a TreeSet internally for both a prefix and a suffix of the classpath. This makes sure that duplicate entries
 * are avoided.
 */
@NotThreadSafe
final class ClassPathBuilder {
  private final LinkedHashSet<String> prefix = new LinkedHashSet<>();
  private final LinkedHashSet<String> suffix = new LinkedHashSet<>();

  /**
   * The oracle that tells us whether a given path could be a YARN configuration path.
   *
   * @param path the path.
   * @return whether or not it could be a YARN conf path.
   */
  private static boolean couldBeYarnConfigurationPath(final String path) {
    return path.contains("conf")
      || path.contains("etc")
      || path.contains(HadoopEnvironment.HADOOP_CONF_DIR);
  }

  /**
   * Adds the given classpath entry. A guess will be made whether it refers to a configuration folder, in which case
   * it will be added to the prefix. Else, it will be added to the suffix.
   *
   * @param classPathEntry the given classpath entry.
   */
  void add(final String classPathEntry) {
    // Make sure that the cluster configuration is in front of user classes
    if (couldBeYarnConfigurationPath(classPathEntry)) {
      this.addToPrefix(classPathEntry);
    } else {
      this.addToSuffix(classPathEntry);
    }
  }

  /**
   * Adds the given classPathEntry to the classpath suffix.
   *
   * @param classPathEntry the given classpath entry.
   */
  void addToSuffix(final String classPathEntry) {
    this.suffix.add(classPathEntry);
  }

  /**
   * Adds the given classPathEntry to the classpath prefix.
   *
   * @param classPathEntry the given classpath entry.
   */
  void addToPrefix(final String classPathEntry) {
    this.prefix.add(classPathEntry);
  }

  /**
   * Adds all entries given using the <code>add()</code> method.
   *
   * @param entries entries to add.
   */
  void addAll(final String... entries) {
    for (final String classPathEntry : entries) {
      this.add(classPathEntry);
    }
  }

  /**
   * Adds all the given entries to the classpath suffix.
   *
   * @param entries entries to add.
   */
  void addAllToSuffix(final String... entries) {
    for (final String classPathEntry : entries) {
      this.addToSuffix(classPathEntry);
    }
  }


  /**
   * @return the suffix in an immutable list.
   */
  List<String> getSuffixAsImmutableList() {
    return Collections.unmodifiableList(new ArrayList<>(this.suffix));
  }

  /**
   * @return the prefix in an immutable list.
   */
  List<String> getPrefixAsImmutableList() {
    return Collections.unmodifiableList(new ArrayList<>(this.prefix));
  }
}
