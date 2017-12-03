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
package edu.snu.onyx.tests.compiler;

import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.compiler.optimizer.policy.DataSkewPolicy;
import edu.snu.onyx.compiler.optimizer.policy.DefaultPolicy;
import edu.snu.onyx.compiler.optimizer.policy.PadoPolicy;
import edu.snu.onyx.compiler.optimizer.policy.SailfishDisaggPolicy;
import edu.snu.onyx.client.JobLauncher;
import edu.snu.onyx.examples.beam.*;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.tests.examples.beam.AlternatingLeastSquareITCase;
import edu.snu.onyx.tests.examples.beam.ArgBuilder;
import edu.snu.onyx.tests.examples.beam.MapReduceITCase;
import edu.snu.onyx.tests.examples.beam.MultinomialLogisticRegressionITCase;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;

import java.lang.reflect.Method;

/**
 * Utility methods for tests.
 */
public final class CompilerTestUtil {
  public static final String rootDir = System.getProperty("user.dir");
  public static final String dagDir = rootDir + "/../dag";
  public static final String padoPolicy = PadoPolicy.class.getCanonicalName();
  public static final String sailfishDisaggPolicy = SailfishDisaggPolicy.class.getCanonicalName();
  public static final String defaultPolicy = DefaultPolicy.class.getCanonicalName();
  public static final String dataSkewPolicy = DataSkewPolicy.class.getCanonicalName();

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

  public static DAG<IRVertex, IREdge> compileMRDAG() throws Exception {
    final ArgBuilder mrArgBuilder = MapReduceITCase.builder;
    return compileDAG(mrArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileALSDAG() throws Exception {
    final ArgBuilder alsArgBuilder = AlternatingLeastSquareITCase.builder;
    return compileDAG(alsArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileALSInefficientDAG() throws Exception {
    final String alsInefficient = "edu.snu.onyx.examples.beam.AlternatingLeastSquareInefficient";
    final String input = rootDir + "/../examples/src/main/resources/sample_input_als";
    final String numFeatures = "10";
    final String numIteration = "3";
    final String dagDirectory = dagDir;

    final ArgBuilder alsArgBuilder = new ArgBuilder()
        .addJobId(AlternatingLeastSquareInefficient.class.getSimpleName())
        .addUserMain(alsInefficient)
        .addUserArgs(input, numFeatures, numIteration)
        .addDAGDirectory(dagDirectory);
    return compileDAG(alsArgBuilder.build());
  }

  public static DAG<IRVertex, IREdge> compileMLRDAG() throws Exception {
    final ArgBuilder mlrArgBuilder = MultinomialLogisticRegressionITCase.builder;
    return compileDAG(mlrArgBuilder.build());
  }
}
