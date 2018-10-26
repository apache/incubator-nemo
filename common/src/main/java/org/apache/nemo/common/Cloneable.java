package org.apache.nemo.common;

/**
 * This interface is implemented by objects that can be cloned.
 * <p>
 * This interface also overcomes the drawback of {@link java.lang.Cloneable} interface which
 * doesn't have a clone method.
 * Josh Bloch (a JDK author) has pointed out this in his book "Effective Java" as
 * <a href="http://thefinestartist.com/effective-java/11"> Override clone judiciously </a>
 * </p>
 *
 * @param <T> the type of objects that this class can clone
 */
public interface Cloneable<T extends Cloneable<T>> {

  /**
   * Creates and returns a copy of this object.
   * <p>
   * The precise meaning of "copy" may depend on the class of the object.
   * The general intent is that, all fields of the object are copied.
   * </p>
   *
   * @return a clone of this object.
   */
  T getClone();
}
