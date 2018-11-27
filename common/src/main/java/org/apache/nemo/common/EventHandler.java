package org.apache.nemo.common;

public interface EventHandler<O> {

  void onNext(O msg);
}
