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
package edu.snu.nemo.common.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test Utils for Examples.
 */
public final class ExampleTestUtil {
  private static final Double ERROR = 1e-8;
  /**
   * Private constructor.
   */
  private ExampleTestUtil() {
  }

  /**
   * Ensures output correctness with the given test resource file.
   *
   * @param resourcePath root folder for both resources.
   * @param outputFileName output file name.
   * @param testResourceFileName the test result file name.
   * @throws IOException IOException while testing.
   */
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

    if (!testOutput.equals(resourceOutput)) {
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
   * This method test the output validity of AlternatingLeastSquareITCase.
   * Due to the floating point math error, the output of the test can be different every time.
   * Thus we cannot compare plain text output, but have to check its numeric error.
   *
   * @param resourcePath path to resources.
   * @param outputFileName name of output file.
   * @param testResourceFileName name of the file to compare the outputs to.
   * @throws IOException exception.
   */
  public static void ensureALSOutputValidity(final String resourcePath,
                                             final String outputFileName,
                                             final String testResourceFileName) throws IOException {
    final List<List<Double>> testOutput = Files.list(Paths.get(resourcePath))
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
        .filter(line -> !line.trim().equals(""))
        .map(line -> Arrays.asList(line.split("\\s*,\\s*"))
            .stream().map(s -> Double.valueOf(s)).collect(Collectors.toList()))
        .collect(Collectors.toList());

    final List<List<Double>> resourceOutput = Files.lines(Paths.get(resourcePath + testResourceFileName))
        .sorted()
        .filter(line -> !line.trim().equals(""))
        .map(line -> Arrays.asList(line.split("\\s*,\\s*"))
            .stream().map(s -> Double.valueOf(s)).collect(Collectors.toList()))
        .collect(Collectors.toList());

    if (testOutput.size() != resourceOutput.size()) {
      throw new RuntimeException("output mismatch");
    }

    IntStream.range(0, testOutput.size()).forEach(i -> {
          IntStream.range(0, testOutput.get(i).size()).forEach(j -> {
            final Double testElement = testOutput.get(i).get(j);
            final Double resourceElement = resourceOutput.get(i).get(j);
            if (Math.abs(testElement - resourceElement) / resourceElement > ERROR) {
              throw new RuntimeException("output mismatch");
            }
          });
        });
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
    for (final Path outputFilePath : outputFilePaths) {
      Files.delete(outputFilePath);
    }
  }


}
