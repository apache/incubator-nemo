package org.apache.nemo.common.ir.executionproperty;

import java.lang.annotation.*;

/**
 * Declares associated {@link ExecutionProperty} for implementations.
 */
@Target({ElementType.TYPE})
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface AssociatedProperty {
  Class<? extends ExecutionProperty> value();
}
