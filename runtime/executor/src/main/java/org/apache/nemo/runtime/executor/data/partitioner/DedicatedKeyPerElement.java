package org.apache.nemo.runtime.executor.data.partitioner;

import java.lang.annotation.*;

/**
 * Declares that all of the designated keys for each element in a {@link Partitioner} is dedicated for the element.
 */
@Target({ElementType.TYPE})
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface DedicatedKeyPerElement {
}
