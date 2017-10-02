package com.devebot.opflow.lab;

/**
 *
 * @author drupalex
 */
public class SimpleCalculatorException extends Exception {
    
    public SimpleCalculatorException() {
    }

    public SimpleCalculatorException(String message) {
        super(message);
    }

    public SimpleCalculatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public SimpleCalculatorException(Throwable cause) {
        super(cause);
    }
    
}
