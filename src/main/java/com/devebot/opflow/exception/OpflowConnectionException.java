package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowConnectionException extends OpflowConstructorException {

    public OpflowConnectionException() {
    }

    public OpflowConnectionException(String message) {
        super(message);
    }

    public OpflowConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowConnectionException(Throwable cause) {
        super(cause);
    }

    public OpflowConnectionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
