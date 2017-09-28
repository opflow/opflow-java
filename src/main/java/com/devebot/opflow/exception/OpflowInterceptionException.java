package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowInterceptionException extends OpflowOperationException {

    public OpflowInterceptionException() {
    }

    public OpflowInterceptionException(String message) {
        super(message);
    }

    public OpflowInterceptionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowInterceptionException(Throwable cause) {
        super(cause);
    }

    public OpflowInterceptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
