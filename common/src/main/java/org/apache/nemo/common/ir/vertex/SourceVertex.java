package org.apache.nemo.common.ir.vertex;

import org.apache.nemo.common.ir.Readable;

import java.util.List;

/**
 * IRVertex that reads data from an external source.
 * It is to be implemented in the compiler frontend with source-specific data fetching logic.
 * @param <O> output type.
 */
public abstract class SourceVertex<O> extends IRVertex {

  /**
   * Constructor for SourceVertex.
   */
  public SourceVertex() {
    super();
  }

  /**
   * Copy Constructor for SourceVertex.
   *
   * @param that the source object for copying
   */
  public SourceVertex(final SourceVertex that) {
    super(that);
  }
  /**
   * Gets parallel readables.
   *
   * @param desiredNumOfSplits number of splits desired.
   * @return the list of readables.
   * @throws Exception if fail to get.
   */
  public abstract List<Readable<O>> getReadables(int desiredNumOfSplits) throws Exception;

  /**
   * Clears internal states, must be called after getReadables().
   * Concretely, this clears the huge list of input splits held by objects like BeamBoundedSourceVertex before
   * sending the vertex to remote executors.
   * Between clearing states of an existing vertex, and creating a new vertex, we've chosen the former approach
   * to ensure consistent use of the same IRVertex object across the compiler, the master, and the executors.
   */
  public abstract void clearInternalStates();
}
