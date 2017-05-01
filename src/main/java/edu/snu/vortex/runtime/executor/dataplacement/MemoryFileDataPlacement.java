package edu.snu.vortex.runtime.executor.dataplacement;

import edu.snu.vortex.compiler.ir.Element;

/**
 * {@inheritDoc}.
 */
public final class MemoryFileDataPlacement implements DataPlacement {
  public MemoryFileDataPlacement() {

  }

  @Override
  public Iterable<Element> get(final String runtimeEdgeId, final int srcTaskIdx, final int dstTaskIdx) {
    return null;
  }

  @Override
  public Iterable<Element> get(final String runtimeEdgeId, final int srcTaskIdx) {
    return null;
  }

  @Override
  public void put(final String runtimeEdgeId, final int srcTaskIdx, final Iterable<Element> data) {

  }

  @Override
  public void put(final String runtimeEdgeId, final int srcTaskIdx,
                  final int partitionIdx, final Iterable<Element> data) {

  }
}
