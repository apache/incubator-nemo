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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.runtime.common.IdGenerator;
import edu.snu.vortex.runtime.common.RtAttributes;
import edu.snu.vortex.runtime.common.RtOperator;

import java.util.HashMap;
import java.util.Map;

/**
 * Operator converter.
 */
public final class OperatorConverter {
  /**
   * Converts an {@link Operator} to its representation in {@link RtOperator}.
   * @param irOp .
   * @return the {@link RtOperator} representation.
   */
  public RtOperator convert(final Operator irOp) {
    final Map<Attributes.Key, Attributes.Val> irOpAttributes = irOp.getAttributes();

    final Map<RtAttributes.RtOpAttribute, Object> rOpAttributes = new HashMap<>();
    irOpAttributes.forEach((k, v) -> {
      switch (k) {
      case Placement:
        if (v == Attributes.Placement.Transient) {
          rOpAttributes.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.TRANSIENT);
        } else if (v == Attributes.Placement.Reserved) {
          rOpAttributes.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.RESERVED);
        } else if (v == Attributes.Placement.Compute) {
          rOpAttributes.put(RtAttributes.RtOpAttribute.RESOURCE_TYPE, RtAttributes.ResourceType.COMPUTE);
        }
        break;
      case EdgePartitioning:
        if (v == Attributes.EdgePartitioning.Hash) {
          rOpAttributes.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.HASH);
        } else if (v == Attributes.EdgePartitioning.Range) {
          rOpAttributes.put(RtAttributes.RtOpAttribute.PARTITION, RtAttributes.Partition.RANGE);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported operator attribute");
      }
    });
    final RtOperator rOp = new RtOperator(irOp.getId(), rOpAttributes);
    return rOp;
  }

  public String convertId(final String irOpId) {
    return IdGenerator.generateRtOpId(irOpId);
  }
}
