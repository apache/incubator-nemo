package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.executionproperty.ExecutionProperty;

import java.lang.annotation.*;

/**
 * Annotation used to indicate which execution properties the class annotates.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Annotates {
  Class<? extends ExecutionProperty>[] value() default {};
}
