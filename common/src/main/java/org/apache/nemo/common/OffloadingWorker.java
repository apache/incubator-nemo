package org.apache.nemo.common;

import java.util.List;

public interface OffloadingWorker {

  void write(Object input);

  void flush();

  void finishOffloading();

  <T> List<T> getResult();
}
