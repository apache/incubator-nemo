package edu.snu.vortex.runtime;

import java.util.List;

public interface Channel<T> {
  void write(List<T> data);

  List<T> read();
}
