package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowWorkerNotFoundException extends OpflowOperationException {

    public OpflowWorkerNotFoundException() {
    }

    public OpflowWorkerNotFoundException(String message) {
        super(message);
    }

    public OpflowWorkerNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowWorkerNotFoundException(Throwable cause) {
        super(cause);
    }

    public OpflowWorkerNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
