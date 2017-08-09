package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowJsonTransformationException extends OpflowOperationException {

    public OpflowJsonTransformationException() {
    }

    public OpflowJsonTransformationException(String message) {
        super(message);
    }

    public OpflowJsonTransformationException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowJsonTransformationException(Throwable cause) {
        super(cause);
    }

    public OpflowJsonTransformationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
