package edu.snu.onyx.runtime.executor.data.filter;

import edu.snu.onyx.common.coder.Coder;

import java.util.List;

public final class Serializer {
  private Coder coder;
  private List<Filter> filters;

  public Serializer(final Coder coder, final List<Filter> filters) {
    this.coder = coder;
    this.filters = filters;
  }

  public Coder getCoder() {
    return coder;
  }

  public void setCoder(final Coder coder) {
    this.coder = coder;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public void setFilters(final List<Filter> filters) {
    this.filters = filters;
  }
}
