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
package edu.snu.nemo.compiler;

import edu.snu.nemo.common.test.ArgBuilder;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;

import java.io.File;
import java.lang.reflect.Method;

/**
 * Utility methods for tests.
 */
public final class CompilerTestUtil {
  private static final String ROOT_DIR = findRoot(System.getProperty("user.dir"));

  // Utility classes should not have a public or default constructor
  private CompilerTestUtil() {

  }

  /**
   * Find the root directory of Nemo project, ascending directory hierarchy one by one starting from {@code curDir}.
   * This method distinguishes the root with "LICENSE" file.
   * @param curDir the current directory
   * @return the absolute path of the root directory
   */
  private static String findRoot(final String curDir) {
    boolean foundRoot = false;
    File folder = new File(curDir);

    while (!foundRoot) {
      folder = folder.getParentFile();
      final File[] listOfFiles = folder.listFiles();
      for (final File file : listOfFiles) {
        if (file.isFile() && file.getName().equals("LICENSE")) {
          foundRoot = true;
          break;
        }
      }
    }

    return folder.getAbsolutePath();
  }

  private static DAG<IRVertex, IREdge> compileDAG(final String[] args) throws Exception {
    final String userMainClassName;
    final String[] userMainMethodArgs;

    try {
      final Configuration configuration = JobLauncher.getJobConf(args);
      final Injector injector = Tang.Factory.getTang().newInjector(configuration);
      userMainClassName = injector.getNamedInstance(JobConf.UserMainClass.class);
      userMainMethodArgs = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");
    } catch (final Exception e) {
      throw new RuntimeException("An exception occurred while processing configuration for invoking user main. "
          + "Note: Using compileDAG for multiple times will fail, as compileDAG method enables static method mocking "
          + "on JobLauncher and because of this Tang may misbehave afterwards.", e);
    }
    final Class userMainClass = Class.forName(userMainClassName);
    final Method userMainMethod = userMainClass.getMethod("main", String[].class);

    final ArgumentCaptor<DAG> captor = ArgumentCaptor.forClass(DAG.class);
    PowerMockito.mockStatic(JobLauncher.class);
    PowerMockito.doNothing().when(JobLauncher.class, "launchDAG", captor.capture());
    userMainMethod.invoke(null, (Object) userMainMethodArgs);
    return captor.getValue();
  }

  public static DAG<IRVertex, IREdge> compileWordCountDAG() throws Exception {
    final String input = ROOT_DIR + "/examples/resources/test_input_wordcount";
    final String output = ROOT_DIR + "/examples/resources/test_output";
    final String main = "edu.snu.nemo.examples.beam.WordCount";

    final ArgBuilder mrArgBuilder = new ArgBuilder()
        .addJobId("WordCount")
        .addUserMain(main)
        .addUserArgs(input, output);
    return compileDAG(mrArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileALSDAG() throws Exception {
    final String input = ROOT_DIR + "/examples/resources/test_input_als";
    final String numFeatures = "10";
    final String numIteration = "3";
    final String main = "edu.snu.nemo.examples.beam.AlternatingLeastSquare";

    final ArgBuilder alsArgBuilder = new ArgBuilder()
        .addJobId("AlternatingLeastSquare")
        .addUserMain(main)
        .addUserArgs(input, numFeatures, numIteration);
    return compileDAG(alsArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileALSInefficientDAG() throws Exception {
    final String input = ROOT_DIR + "/examples/resources/test_input_als";
    final String numFeatures = "10";
    final String numIteration = "3";
    final String main = "edu.snu.nemo.examples.beam.AlternatingLeastSquareInefficient";

    final ArgBuilder alsArgBuilder = new ArgBuilder()
        .addJobId("AlternatingLeastSquareInefficient")
        .addUserMain(main)
        .addUserArgs(input, numFeatures, numIteration);
    return compileDAG(alsArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileMLRDAG() throws Exception {
    final String input = ROOT_DIR + "/examples/resources/test_input_mlr";
    final String numFeatures = "100";
    final String numClasses = "5";
    final String numIteration = "3";
    final String main = "edu.snu.nemo.examples.beam.MultinomialLogisticRegression";

    final ArgBuilder mlrArgBuilder = new ArgBuilder()
        .addJobId("MultinomialLogisticRegression")
        .addUserMain(main)
        .addUserArgs(input, numFeatures, numClasses, numIteration);
    return compileDAG(mlrArgBuilder.build());
  }
}
