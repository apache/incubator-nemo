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
package org.apache.nemo.examples.beam;

import com.google.common.collect.Lists;
import org.apache.nemo.compiler.frontend.beam.NemoPipelineOptions;
import org.apache.nemo.compiler.frontend.beam.NemoRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.io.Serializable;
import java.util.List;

/**
 * Per percentile statistics application.
 */
public final class PerPercentileAverage {
  /**
   * Private Constructor.
   */
  private PerPercentileAverage() {
  }

  /**
   * Main function for the MR BEAM program.
   *
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoRunner.class);
    options.setJobName("PerPercentileAverage");

    final Pipeline p = Pipeline.create(options);

    PCollection<Student> students = GenericSourceSink.read(p, inputFilePath)
        .apply(ParDo.of(new DoFn<String, Student>() {
          @ProcessElement
          public void processElement(final ProcessContext c) {
            String[] line = c.element().split(" ");
            c.output(new Student(Integer.parseInt(line[0]), Integer.parseInt(line[1]), Integer.parseInt(line[2])));
          }
        }))
        .setCoder(SerializableCoder.of(Student.class));

    PCollectionList<Student> studentsByPercentile =
        // Make sure that each partition contain at least one element.
        // If there are empty PCollections, successive WriteFiles may fail.
        students.apply(Partition.of(10, new Partition.PartitionFn<Student>() {
          public int partitionFor(final Student student, final int numPartitions) {
            return student.getPercentile() / numPartitions;
          }
        }));

    PCollection<String> [] results  = new PCollection[10];
    for (int i = 0; i < 10; i++) {
      results[i] = studentsByPercentile.get(i)
          .apply(MapElements.via(new SimpleFunction<Student, KV<String, Integer>>() {
            @Override
            public KV<String, Integer> apply(final Student student) {
              return KV.of("", student.getScore());
            }
          }))
          .apply(GroupByKey.create())
          .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<Integer>>, String>() {
            @Override
            public String apply(final KV<String, Iterable<Integer>> kv) {
              List<Integer> scores = Lists.newArrayList(kv.getValue());
              final int sum = scores.stream().reduce(0, (Integer x, Integer y) -> x + y);
              return scores.size() + " " + (double) sum / scores.size();
            }
          }));
      GenericSourceSink.write(results[i], outputFilePath + "_" + i);
    }

    p.run();
  }

  /**
   * Student Class.
   */
  static class Student implements Serializable {
    private int id;
    private int percentile;
    private int score;

    /**
     * Constructor.
     * @param id student id.
     * @param percentile student percentile.
     * @param score student score.
     */
    Student(final int id, final int percentile, final int score) {
      this.id = id;
      this.percentile = percentile;
      this.score = score;
    }

    /**
     * Getter for student id.
     * @return id.
     */
    public int getId() {
      return id;
    }

    /**
     * Setter for student id.
     * @param id id.
     */
    public void setId(final int id) {
      this.id = id;
    }

    /**
     * Getter for student percentile.
     * @return percentile.
     */
    public int getPercentile() {
      return percentile;
    }

    /**
     * Setter for student percentile.
     * @param percentile percentile.
     */
    public void setPercentile(final int percentile) {
      this.percentile = percentile;
    }

    /**
     * Getter for student score.
     * @return score.
     */
    public int getScore() {
      return score;
    }

    /**
     * Setter for student score.
     * @param score score.
     */
    public void setScore(final int score) {
      this.score = score;
    }
  }
}
