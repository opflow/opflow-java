package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowBootstrapException extends Exception {

    public OpflowBootstrapException() {
    }

    public OpflowBootstrapException(String message) {
        super(message);
    }

    public OpflowBootstrapException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowBootstrapException(Throwable cause) {
        super(cause);
    }

    public OpflowBootstrapException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
