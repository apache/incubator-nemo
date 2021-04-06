package org.apache.nemo.common.ir;

import org.apache.nemo.common.ir.vertex.IRVertex;

import java.util.HashMap;
import java.util.Map;

public final class PassSharedData {

  public static final Map<IRVertex, IRVertex> originVertexToTransientVertexMap = new HashMap<>();
}
