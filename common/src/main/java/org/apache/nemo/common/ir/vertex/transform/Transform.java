package org.apache.nemo.common.ir.vertex.transform;

import org.apache.nemo.common.ir.OutputCollector;
import java.io.Serializable;
import java.util.Optional;

/**
 * Interface for specifying 'What' to do with data.
 * It is to be implemented in the compiler frontend, possibly for every operator in a dataflow language.
 * 'How' and 'When' to do with its input/output data are up to the runtime.
 * @param <I> input type.
 * @param <O> output type.
 */
public interface Transform<I, O> extends Serializable {
  /**
   * Prepare the transform.
   * @param context of the transform.
   * @param outputCollector that collects outputs.
   */
  void prepare(Context context, OutputCollector<O> outputCollector);

  /**
   * On data received.
   * @param element data received.
   */
  void onData(I element);

  /**
   * Close the transform.
   */
  void close();

  /**
   * Context of the transform.
   */
  interface Context extends Serializable {
    /**
     * @return the broadcast variable.
     */
    Object getBroadcastVariable(Serializable id);

    /**
     * Put serialized data to send to the executor.
     * @param serializedData the serialized data.
     */
    void setSerializedData(String serializedData);

    /**
     * Retrieve the serialized data on the executor.
     * @return the serialized data.
     */
    Optional<String> getSerializedData();
  }
}
