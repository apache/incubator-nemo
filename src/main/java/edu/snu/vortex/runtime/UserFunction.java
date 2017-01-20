package edu.snu.vortex.runtime;

import java.util.List;

public interface UserFunction<I, O> {
  List<O> func(List<I> input);
}
