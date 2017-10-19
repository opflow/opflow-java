package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowUnimplementedException extends OpflowOperationException {

    public OpflowUnimplementedException() {
    }

    public OpflowUnimplementedException(String message) {
        super(message);
    }

    public OpflowUnimplementedException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowUnimplementedException(Throwable cause) {
        super(cause);
    }

    public OpflowUnimplementedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
