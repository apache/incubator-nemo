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
package edu.snu.nemo.compiler.frontend.spark.core.java;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.compiler.frontend.spark.SparkKeyExtractor;
import edu.snu.nemo.compiler.frontend.spark.coder.SparkCoder;
import edu.snu.nemo.compiler.frontend.spark.transform.CollectTransform;
import edu.snu.nemo.compiler.frontend.spark.transform.GroupByKeyTransform;
import edu.snu.nemo.compiler.frontend.spark.transform.ReduceByKeyTransform;
import org.apache.spark.SparkContext;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Utility class for RDDs.
 */
public final class SparkFrontendUtils {
  /**
   * Private constructor.
   */
  private SparkFrontendUtils() {
  }

  /**
   * Derive Spark serializer from a spark context.
   * @param sparkContext spark context to derive the serializer from.
   * @return the serializer.
   */
  public static Serializer deriveSerializerFrom(final SparkContext sparkContext) {
    if (sparkContext.conf().get("spark.serializer", "")
        .equals("org.apache.spark.serializer.KryoSerializer")) {
      return new KryoSerializer(sparkContext.conf());
    } else {
      return new JavaSerializer(sparkContext.conf());
    }
  }

  /**
   * Collect data by running the DAG.
   * @param dag the DAG to execute.
   * @param loopVertexStack loop vertex stack.
   * @param lastVertex last vertex added to the dag.
   * @param serializer serializer for the edges.
   * @param <T> type of the return data.
   * @return the data collected.
   */
  public static <T> List<T> collect(final DAG<IRVertex, IREdge> dag, final Stack<LoopVertex> loopVertexStack,
                                    final IRVertex lastVertex, final Serializer serializer) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);

    // save result in a temporary file
    // TODO #740: remove this part, and make it properly transfer with executor.
    final String resultFile = System.getProperty("user.dir") + "/collectresult";

    final IRVertex collectVertex = new OperatorVertex(new CollectTransform<>(resultFile));
    builder.addVertex(collectVertex, loopVertexStack);

    final IREdge newEdge = new IREdge(getEdgeCommunicationPattern(lastVertex, collectVertex),
        lastVertex, collectVertex, new SparkCoder(serializer));
    newEdge.setProperty(KeyExtractorProperty.of(new SparkKeyExtractor()));
    builder.connectVertices(newEdge);

    // launch DAG
    JobLauncher.launchDAG(builder.build());

    // Retrieve result data from file.
    // TODO #740: remove this part, and make it properly transfer with executor.
    try {
      final List<T> result = new ArrayList<>();
      Integer i = 0;

      // TODO #740: remove this part, and make it properly transfer with executor.
      File file = new File(resultFile + i);
      while (file.exists()) {
        try (final FileInputStream fin = new FileInputStream(file)) {
          try (final ObjectInputStream ois = new ObjectInputStream(fin)) {
            result.addAll((List<T>) ois.readObject());
          }
        }

        // Delete temporary file
        if (file.delete()) {
          file = new File(resultFile + ++i);
        }
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieve communication pattern of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   * @return the communication pattern.
   */
  static DataCommunicationPatternProperty.Value getEdgeCommunicationPattern(final IRVertex src,
                                                                            final IRVertex dst) {
    if (dst instanceof OperatorVertex
        && (((OperatorVertex) dst).getTransform() instanceof ReduceByKeyTransform
        || ((OperatorVertex) dst).getTransform() instanceof GroupByKeyTransform)) {
      return DataCommunicationPatternProperty.Value.Shuffle;
    } else {
      return DataCommunicationPatternProperty.Value.OneToOne;
    }
  }
}
