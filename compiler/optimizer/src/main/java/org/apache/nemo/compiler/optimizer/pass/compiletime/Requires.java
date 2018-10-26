package org.apache.nemo.compiler.optimizer.pass.compiletime;

import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;

import java.lang.annotation.*;

/**
 * Annotation used to indicate which execution properties the class requires as prerequisites.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Requires {
  Class<? extends ExecutionProperty>[] value() default {};
}
