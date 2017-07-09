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
package edu.snu.vortex.compiler;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.examples.beam.*;
import edu.snu.vortex.common.dag.DAG;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility methods for tests.
 */
public final class TestUtil {
  public static String rootDir = System.getProperty("user.dir");

  public static DAG<IRVertex, IREdge> compileMRDAG() throws Exception {
    final Frontend beamFrontend = new BeamFrontend();
    final ArgBuilder mrArgBuilder = MapReduceITCase.builder;
    final Configuration configuration = JobLauncher.getJobConf(mrArgBuilder.build());
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String[] arguments = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");

    return beamFrontend.compile(className, arguments);
  }

  public static DAG<IRVertex, IREdge> compileALSDAG() throws Exception {
    final Frontend beamFrontend = new BeamFrontend();
    final ArgBuilder alsArgBuilder = AlternatingLeastSquareITCase.builder;
    final Configuration configuration = JobLauncher.getJobConf(alsArgBuilder.build());
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String[] arguments = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");

    return beamFrontend.compile(className, arguments);
  }

  public static DAG<IRVertex, IREdge> compileALSInefficientDAG() throws Exception {
    final String alsInefficient = "edu.snu.vortex.examples.beam.AlternatingLeastSquareInefficient";
    final String input = rootDir + "/src/main/resources/sample_input_als";
    final String numFeatures = "10";
    final String numIteration = "3";
    final String dagDirectory = "./dag";

    final Frontend beamFrontend = new BeamFrontend();
    final ArgBuilder alsArgBuilder = new ArgBuilder()
        .addJobId(AlternatingLeastSquareInefficient.class.getSimpleName())
        .addUserMain(alsInefficient)
        .addUserArgs(input, numFeatures, numIteration)
        .addDAGDirectory(dagDirectory);
    final Configuration configuration = JobLauncher.getJobConf(alsArgBuilder.build());
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String[] arguments = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");

    return beamFrontend.compile(className, arguments);
  }

  public static DAG<IRVertex, IREdge> compileMLRDAG() throws Exception {
    final Frontend beamFrontend = new BeamFrontend();
    final ArgBuilder alsArgBuilder = MultinomialLogisticRegressionITCase.builder;
    final Configuration configuration = JobLauncher.getJobConf(alsArgBuilder.build());
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String[] arguments = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");

    return beamFrontend.compile(className, arguments);
  }

  public static final class EmptyBoundedSource extends BoundedSource {
    private final String name;

    public EmptyBoundedSource(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    public BoundedReader createReader(PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException("Empty bounded source");
    }

    @Override
    public List<? extends BoundedSource> split(final long l, final PipelineOptions pipelineOptions) throws Exception {
      return Arrays.asList(this);
    }

    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 1;
    }

    public List<? extends BoundedSource> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return new ArrayList<>();
    }

    public void validate() {
    }

    public Coder getDefaultOutputCoder() {
      throw new UnsupportedOperationException("Empty bounded source");
    }
  }

  public static final class EmptyTransform implements Transform {
    private final String name;

    public EmptyTransform(final String name) {
      this.name = name;
    }

    @Override
    public final String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append(super.toString());
      sb.append(", name: ");
      sb.append(name);
      return sb.toString();
    }

    @Override
    public void prepare(final Context context, final OutputCollector outputCollector) {
    }

    @Override
    public void onData(final Iterable<Element> data, final String srcVertexId) {
    }

    @Override
    public void close() {
    }
  }
}
