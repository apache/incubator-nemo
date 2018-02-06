package edu.snu.coral.runtime.executor.data.streamchainer;

import edu.snu.coral.common.coder.Coder;

import java.util.List;

/**
 * class that contains {@link Coder} and {@link List< StreamChainer >}.
 * @param <T> coder element type.
 */
public final class Serializer<T> {
  private Coder<T> coder;
  private List<StreamChainer> streamChainers;

  /**
   * Constructor.
   *
   * @param coder      {@link Coder}.
   * @param streamChainers list of {@link StreamChainer}.
   */
  public Serializer(final Coder<T> coder, final List<StreamChainer> streamChainers) {
    this.coder = coder;
    this.streamChainers = streamChainers;
  }

  /**
   * method that returns {@link Coder}.
   *
   * @return {@link Coder}.
   */
  public Coder<T> getCoder() {
    return coder;
  }

  /**
   * method that sets {@link Coder}.
   *
   * @param coder {@link Coder}.
   */
  public void setCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  /**
   * method that returns list of {@link StreamChainer}.
   *
   * @return list of {@link StreamChainer}.
   */
  public List<StreamChainer> getStreamChainers() {
    return streamChainers;
  }

  /**
   * method that sets list of {@link StreamChainer}.
   *
   * @param streamChainers list of {@link StreamChainer}.
   */
  public void setStreamChainers(final List<StreamChainer> streamChainers) {
    this.streamChainers = streamChainers;
  }
}
