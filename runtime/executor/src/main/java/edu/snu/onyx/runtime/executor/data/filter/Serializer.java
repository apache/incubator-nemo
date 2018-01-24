package edu.snu.onyx.runtime.executor.data.filter;

import edu.snu.onyx.common.coder.Coder;

import java.util.List;

public final class Serializer<T> {
  private Coder<T> coder;
  private List<Filter> filters;

  public Serializer(final Coder<T> coder, final List<Filter> filters) {
    this.coder = coder;
    this.filters = filters;
  }

  public Coder<T> getCoder() {
    return coder;
  }

  public void setCoder(final Coder<T> coder) {
    this.coder = coder;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public void setFilters(final List<Filter> filters) {
    this.filters = filters;
  }
}
