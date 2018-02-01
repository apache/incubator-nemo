/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.examples.beam;

import edu.snu.onyx.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import edu.snu.onyx.compiler.optimizer.policy.Policy;
import edu.snu.onyx.runtime.common.optimizer.pass.runtime.RuntimePass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class ExampleTestUtil {
  public static void ensureOutputValidity(final String resourcePath,
                                          final String outputFileName,
                                          final String testResourceFileName)
  throws IOException {
    final String testOutput = Files.list(Paths.get(resourcePath))
        .filter(Files::isRegularFile)
        .filter(path -> path.getFileName().toString().startsWith(outputFileName))
        .flatMap(path -> {
          try {
            return Files.lines(path);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        })
        .sorted()
        .reduce("", (p, q) -> (p + "\n" + q));

    final String resourceOutput = Files.lines(Paths.get(resourcePath + testResourceFileName))
        .sorted()
        .reduce("", (p, q) -> (p + "\n" + q));

    if(!testOutput.equals(resourceOutput)) {
      final String outputMsg =
          "Test output mismatch while comparing [" + outputFileName + "] from [" + testResourceFileName + "] under "
              + resourcePath + ":\n"
              + "=============" + outputFileName + "=================="
              + testOutput
              + "\n=============" + testResourceFileName + "=================="
              + resourceOutput
              + "\n===============================";
      throw new RuntimeException(outputMsg);
    }
  }

  /**
   * Delete output files.
   *
   * @param directory      the path of file directory.
   * @param outputFileName the output file prefix.
   * @throws IOException if fail to delete.
   */
  public static void deleteOutputFile(final String directory,
                                      final String outputFileName) throws IOException {
    final Set<Path> outputFilePaths = Files.list(Paths.get(directory))
        .filter(Files::isRegularFile)
        .filter(path -> path.getFileName().toString().startsWith(outputFileName))
        .collect(Collectors.toSet());
    for(final Path outputFilePath : outputFilePaths) {
      Files.delete(outputFilePath);
    }
  }

  /**
   * Overwrite the parallelism of existing policy.
   *
   * @param desiredSourceParallelism       the desired source parallelism to set.
   * @param policyToOverwriteCanonicalName the name of the policy to overwrite parallelism.
   * @return the overwritten policy.
   */
  public static Policy overwriteParallelism(final int desiredSourceParallelism,
                                            final String policyToOverwriteCanonicalName) {
    final Policy policyToOverwrite;
    try {
      policyToOverwrite = (Policy) Class.forName(policyToOverwriteCanonicalName).newInstance();
    } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    final List<CompileTimePass> compileTimePasses = policyToOverwrite.getCompileTimePasses();
    final int parallelismPassIdx = compileTimePasses.indexOf(new DefaultParallelismPass());
    compileTimePasses.set(parallelismPassIdx, new DefaultParallelismPass(desiredSourceParallelism, 2));
    final List<RuntimePass<?>> runtimePasses = policyToOverwrite.getRuntimePasses();

    return new Policy() {
      @Override
      public List<CompileTimePass> getCompileTimePasses() {
        return compileTimePasses;
      }

      @Override
      public List<RuntimePass<?>> getRuntimePasses() {
        return runtimePasses;
      }
    };
  }
}
