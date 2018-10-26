package org.apache.nemo.common.eventhandler;

/**
 * Class for handling events sent from Compiler.
 * @param <T> type of the compiler event to handle.
 */
public interface CompilerEventHandler<T extends CompilerEvent> extends CommonEventHandler<T> {
}
