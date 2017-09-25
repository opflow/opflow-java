package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowInterceptorException extends OpflowOperationException {

    public OpflowInterceptorException() {
    }

    public OpflowInterceptorException(String message) {
        super(message);
    }

    public OpflowInterceptorException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowInterceptorException(Throwable cause) {
        super(cause);
    }

    public OpflowInterceptorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
