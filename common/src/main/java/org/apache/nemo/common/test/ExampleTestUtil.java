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
package org.apache.nemo.common.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
   * @param expectedFileName the test result file name.
   * @throws RuntimeException if the output is invalid.
   */
  public static void ensureOutputValidity(final String resourcePath,
                                          final String outputFileName,
                                          final String expectedFileName) throws IOException {
    final String testOutput =
      getSortedLineStream(resourcePath, outputFileName).reduce("", (p, q) -> (p + "\n" + q));
    final String expectedOutput =
      getSortedLineStream(resourcePath, expectedFileName).reduce("", (p, q) -> (p + "\n" + q));
    if (!testOutput.equals(expectedOutput)) {
      final String outputMsg =
        "Test output mismatch while comparing [" + outputFileName + "] from [" + expectedFileName + "] under "
          + resourcePath + ":\n"
          + "=============" + outputFileName + "=================="
          + testOutput
          + "\n=============" + expectedFileName + "=================="
          + expectedOutput
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
   * @param expectedFileName name of the file to compare the outputs to.
   * @throws RuntimeException if the output is invalid.
   * @throws IOException exception.
   */
  public static void ensureALSOutputValidity(final String resourcePath,
                                             final String outputFileName,
                                             final String expectedFileName) throws IOException {
    final List<List<Double>> testOutput = getSortedLineStream(resourcePath, outputFileName)
      .filter(line -> !line.trim().equals(""))
      .map(line -> Arrays.asList(line.split("\\s*,\\s*"))
        .stream().map(s -> Double.valueOf(s)).collect(Collectors.toList()))
      .collect(Collectors.toList());

    final List<List<Double>> expectedOutput = getSortedLineStream(resourcePath, expectedFileName)
      .filter(line -> !line.trim().equals(""))
      .map(line -> Arrays.asList(line.split("\\s*,\\s*"))
        .stream().map(s -> Double.valueOf(s)).collect(Collectors.toList()))
      .collect(Collectors.toList());

    if (testOutput.size() != expectedOutput.size()) {
      throw new RuntimeException(testOutput.size() + " is not " + expectedOutput.size());
    }

    for (int i = 0; i < testOutput.size(); i++) {
      for (int j = 0; j < testOutput.get(i).size(); j++) {
        final Double testElement = testOutput.get(i).get(j);
        final Double expectedElement = expectedOutput.get(i).get(j);
        if (Math.abs(testElement - expectedElement) / expectedElement > ERROR) {
          throw new RuntimeException("output mismatch");
        }
      }
    }
  }

  public static void ensureSQLOutputValidity(final String resourcePath,
                                             final String outputFileName,
                                             final String expectedFileName) throws IOException {
    final List<List<String>> testOutput = getSortedLineStream(resourcePath, outputFileName)
      .map(line -> Arrays.asList(line.split("\\|")))
      .collect(Collectors.toList());

    final List<List<String>> expectedOutput = getSortedLineStream(resourcePath, expectedFileName)
      .map(line -> Arrays.asList(line.split("\\|")))
      .collect(Collectors.toList());

    if (testOutput.size() != expectedOutput.size()) {
      throw new RuntimeException(testOutput.size() + " is not " + expectedOutput.size());
    }

    for (int i = 0; i < testOutput.size(); i++) {
      for (int j = 0; j < testOutput.get(i).size(); j++) {
        final String testElement = testOutput.get(i).get(j);
        final String expectedElement = expectedOutput.get(i).get(j);

        try {
          // This element is double: Account for floating errors
          final double testElementDouble = Double.valueOf(testElement);
          final double expectedElementDouble = Double.valueOf(expectedElement);
          if (Math.abs(testElementDouble - expectedElementDouble) / expectedElementDouble > ERROR) {
            throw new RuntimeException(testElement + " is not " + expectedElement);
          }
        } catch (NumberFormatException e) {
          // This element is not double: Simply compare the strings
          if (!testElement.equals(expectedElement)) {
            throw new RuntimeException(testElement + " is not " + expectedElement);
          }
        }
      }
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
    try (final Stream<Path> fileStream = Files.list(Paths.get(directory))) {
      final Set<Path> outputFilePaths = fileStream
        .filter(Files::isRegularFile)
        .filter(path -> path.getFileName().toString().startsWith(outputFileName))
        .collect(Collectors.toSet());
      for (final Path outputFilePath : outputFilePaths) {
        Files.delete(outputFilePath);
      }
    }
  }

  private static Stream<String> getSortedLineStream(final String directory, final String fileNameStartsWith) {
    try {
      return Files.list(Paths.get(directory))
        .filter(Files::isRegularFile)
        .filter(path -> path.getFileName().toString().startsWith(fileNameStartsWith))
        .flatMap(path -> {
          try {
            return Files.lines(path);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        })
        .sorted();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
