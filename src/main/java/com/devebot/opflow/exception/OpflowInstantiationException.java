package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowInstantiationException extends OpflowConstructionException {

    public OpflowInstantiationException() {
    }

    public OpflowInstantiationException(String message) {
        super(message);
    }

    public OpflowInstantiationException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowInstantiationException(Throwable cause) {
        super(cause);
    }

    public OpflowInstantiationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
