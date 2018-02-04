package edu.snu.coral.runtime.executor.data.filter;

import edu.snu.coral.common.coder.Coder;

import java.util.List;

/**
 * class that contains {@link Coder} and {@link List<Chainable>}.
 * @param <T> coder element type.
 */
public final class Serializer<T> {
  private Coder<T> coder;
  private List<Chainable> chainables;

  /**
   * Constructor.
   *
   * @param coder      {@link Coder}.
   * @param chainables list of {@link Chainable}.
   */
  public Serializer(final Coder<T> coder, final List<Chainable> chainables) {
    this.coder = coder;
    this.chainables = chainables;
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
   * method that returns list of {@link Chainable}.
   *
   * @return list of {@link Chainable}.
   */
  public List<Chainable> getChainables() {
    return chainables;
  }

  /**
   * method that sets list of {@link Chainable}.
   *
   * @param chainables list of {@link Chainable}.
   */
  public void setChainables(final List<Chainable> chainables) {
    this.chainables = chainables;
  }
}
