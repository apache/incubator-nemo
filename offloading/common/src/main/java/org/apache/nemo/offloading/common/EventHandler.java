package org.apache.nemo.offloading.common;

public interface EventHandler<O> {

  void onNext(O msg);
}
