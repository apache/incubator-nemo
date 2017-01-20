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
package edu.snu.vortex.runtime.common.example;

import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.exception.NoSuchRtStageException;

import java.util.HashMap;
import java.util.Map;

public final class SimpleExecutionPlan {
  public static void main (String[] args) {
    final ExecutionPlan simplePlan = new ExecutionPlan();

    /** A simple Execution Plan composed of 3 stages, Stage A and B independent of each other,
     * while Stage C depends on both A and B.
     * Operator a2 is connected to Operator b1 and Operator c1.
     */

    // Make Stage A
    final Map<RtAttributes.RtStageAttribute, Object> stageA_Attr = new HashMap<>();
    stageA_Attr.put(RtAttributes.RtStageAttribute.PARALLELISM, 3);
    final RtStage a = new RtStage(stageA_Attr);

    final String mockIrOpIdA1 = "a1";
    final Map<RtAttributes.RtOpAttribute, Object> rtOpA1_Attr = new HashMap<>();
    rtOpA1_Attr.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.HASH);
    rtOpA1_Attr.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.TRANSIENT);
    final RtOperator a1 = new RtOperator(mockIrOpIdA1, rtOpA1_Attr, null);

    final String mockIrOpIdA2 = "a2";
    final Map<RtAttributes.RtOpAttribute, Object> rtOpA2_Attr = new HashMap<>();
    rtOpA2_Attr.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.RANGE);
    rtOpA2_Attr.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.RESERVED);
    final RtOperator a2 = new RtOperator(mockIrOpIdA2, rtOpA2_Attr, null);

    a.addRtOp(a1);
    a.addRtOp(a2);

    final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkA12_Attr = new HashMap<>();
    rtOpLinkA12_Attr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.LOCAL_MEM);
    rtOpLinkA12_Attr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, RtAttributes.CommPattern.ONE_TO_ONE);
    final RtOpLink a1_a2 = new RtOpLink(a1, a2, rtOpLinkA12_Attr);
    a.connectRtOps(a1.getId(), a2.getId(), a1_a2);

    // Make Stage B
    final Map<RtAttributes.RtStageAttribute, Object> stageB_Attr = new HashMap<>();
    stageB_Attr.put(RtAttributes.RtStageAttribute.PARALLELISM, 2);
    final RtStage b = new RtStage(stageB_Attr);

    final String mockIrOpIdB1 = "b1";
    final Map<RtAttributes.RtOpAttribute, Object> rtOpB1_Attr = new HashMap<>();
    rtOpB1_Attr.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.HASH);
    rtOpB1_Attr.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.TRANSIENT);
    final RtOperator b1 = new RtOperator(mockIrOpIdB1, rtOpB1_Attr, null);

    b.addRtOp(b1);

    // Make Stage C
    final Map<RtAttributes.RtStageAttribute, Object> stageC_Attr = new HashMap<>();
    stageC_Attr.put(RtAttributes.RtStageAttribute.PARALLELISM, 4);
    final RtStage c = new RtStage(stageC_Attr);

    final String mockIrOpIdC1 = "c1";
    final Map<RtAttributes.RtOpAttribute, Object> rtOpC1_Attr = new HashMap<>();
    rtOpC1_Attr.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.HASH);
    rtOpC1_Attr.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.TRANSIENT);
    final RtOperator c1 = new RtOperator(mockIrOpIdC1, rtOpC1_Attr, null);

    c.addRtOp(c1);

    final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkA2B1_Attr = new HashMap<>();
    rtOpLinkA2B1_Attr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.FILE);
    rtOpLinkA2B1_Attr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, RtAttributes.CommPattern.SCATTER_GATHER);
    final RtOpLink a2_c1 = new RtOpLink(a2, c1, rtOpLinkA2B1_Attr);

    final Map<RtAttributes.RtOpLinkAttribute, Object> rtOpLinkA2C1_Attr = new HashMap<>();
    rtOpLinkA2C1_Attr.put(RtAttributes.RtOpLinkAttribute.CHANNEL, RtAttributes.Channel.FILE);
    rtOpLinkA2C1_Attr.put(RtAttributes.RtOpLinkAttribute.COMM_PATTERN, RtAttributes.CommPattern.SCATTER_GATHER);
    final RtOpLink b1_c1 = new RtOpLink(b1, c1, rtOpLinkA2C1_Attr);

    // Add stages to the execution plan
    simplePlan.addRtStage(a);
    simplePlan.addRtStage(b);
    simplePlan.addRtStage(c);

    // Connect the stages with links a2_c1 and b1_c1
    try {
      simplePlan.connectRtStages(a, c, a2_c1);
      simplePlan.connectRtStages(b, c, b1_c1);
    } catch (final NoSuchRtStageException ex) {
    }
  }
}
