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
package edu.snu.vortex.runtime.utils;


import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.exception.UnsupportedAttributeException;


/**
 * Runtime attribute converter functions.
 */
public final class RuntimeAttributeConverter {

  private RuntimeAttributeConverter() {

  }

  /**
   * Converts IR's IRVertex Attributes to Runtime's attributes.
   * @param irAttributes attributes to convert.
   * @return a map of Runtime attributes.
   */
  public static RuntimeAttributeMap convertVertexAttributes(
      final AttributeMap irAttributes) {
    final RuntimeAttributeMap runtimeVertexAttributes = new RuntimeAttributeMap();

    irAttributes.forEachAttr(((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Placement:
          final RuntimeAttribute runtimeAttributeVal;
          switch (irAttributeVal) {
            case None:
              runtimeAttributeVal = RuntimeAttribute.None;
              break;
            case Transient:
              runtimeAttributeVal = RuntimeAttribute.Transient;
              break;
            case Reserved:
              runtimeAttributeVal = RuntimeAttribute.Reserved;
              break;
            case Compute:
              runtimeAttributeVal = RuntimeAttribute.Compute;
              break;
            case Storage:
              runtimeAttributeVal = RuntimeAttribute.Storage;
              break;
            default:
              throw new UnsupportedAttributeException(
                  "\'" + irAttributeVal + "\' can not be a value of " + irAttributeKey);
          }
          runtimeVertexAttributes.put(RuntimeAttribute.Key.ContainerType, runtimeAttributeVal);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute (" + irAttributeKey + ") is not supported.");
      }
    }));

    irAttributes.forEachIntAttr((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Parallelism:
          runtimeVertexAttributes.put(RuntimeAttribute.IntegerKey.Parallelism, irAttributeVal);
          break;
        default:
          throw new UnsupportedAttributeException("this IR attribute (" + irAttributeKey + ") is not supported.");
      }
    });

    return runtimeVertexAttributes;
  }

  /**
   * Converts IR's IREdge Attributes to Runtime's attributes.
   * @param irAttributes attributes to convert.
   * @return a map of Runtime attributes.
   */
  public static RuntimeAttributeMap convertEdgeAttributes(
      final AttributeMap irAttributes) {
    final RuntimeAttributeMap runtimeEdgeAttributes = new RuntimeAttributeMap();

    irAttributes.forEachAttr(((irAttributeKey, irAttributeVal) -> {
      switch (irAttributeKey) {
        case Partitioning:
          final RuntimeAttribute partitioningAttrVal;
          switch (irAttributeVal) {
            case Hash:
              partitioningAttrVal = RuntimeAttribute.Hash;
              break;
            case Range:
              partitioningAttrVal = RuntimeAttribute.Range;
              break;
            default:
              throw new UnsupportedAttributeException(
                  "\'" + irAttributeVal + "\' can not be a value of " + irAttributeKey);
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.Partition, partitioningAttrVal);
          break;
        case ChannelDataPlacement:
          final RuntimeAttribute channelPlacementAttrVal;
          switch (irAttributeVal) {
            case Local:
              channelPlacementAttrVal = RuntimeAttribute.Local;
              break;
            case Memory:
              channelPlacementAttrVal = RuntimeAttribute.Memory;
              break;
            case File:
              channelPlacementAttrVal = RuntimeAttribute.File;
              break;
            case DistributedStorage:
              channelPlacementAttrVal = RuntimeAttribute.DistributedStorage;
              break;
            default:
              throw new UnsupportedAttributeException(
                  "\'" + irAttributeVal + "\' can not be a value of " + irAttributeKey);
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.PartitionStore, channelPlacementAttrVal);
          break;
        case ChannelTransferPolicy:
          final RuntimeAttribute channelTransferPolicyAttrVal;
          switch (irAttributeVal) {
            case Pull:
              channelTransferPolicyAttrVal = RuntimeAttribute.Pull;
              break;
            case Push:
              channelTransferPolicyAttrVal = RuntimeAttribute.Push;
              break;
            default:
              throw new UnsupportedAttributeException(
                  "\'" + irAttributeVal + "\' can not be a value of " + irAttributeKey);
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.PullOrPush, channelTransferPolicyAttrVal);
          break;
        case CommunicationPattern:
          final RuntimeAttribute commPatternAttrVal;
          switch (irAttributeVal) {
            case OneToOne:
              commPatternAttrVal = RuntimeAttribute.OneToOne;
              break;
            case Broadcast:
              commPatternAttrVal = RuntimeAttribute.Broadcast;
              break;
            case ScatterGather:
              commPatternAttrVal = RuntimeAttribute.ScatterGather;
              break;
            default:
              throw new UnsupportedAttributeException(
                  "\'" + irAttributeVal + "\' can not be a value of " + irAttributeKey);

          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.CommPattern, commPatternAttrVal);
          break;
        case SideInput:
          final RuntimeAttribute sideInput;
          switch (irAttributeVal) {
            case SideInput:
              sideInput = RuntimeAttribute.SideInput;
              break;
            default:
              throw new UnsupportedAttributeException(
                  "\'" + irAttributeVal + "\' can not be a value of " + irAttributeKey);
          }

          runtimeEdgeAttributes.put(RuntimeAttribute.Key.SideInput, sideInput);
          break;
        default:
          throw new UnsupportedAttributeException("This IR attribute (" + irAttributeKey + ") is not supported");
      }
    }));
    return runtimeEdgeAttributes;
  }
}
