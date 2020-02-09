package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRequestPausingException extends OpflowOperationException {

    public OpflowRequestPausingException() {
    }

    public OpflowRequestPausingException(String message) {
        super(message);
    }

    public OpflowRequestPausingException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRequestPausingException(Throwable cause) {
        super(cause);
    }

    public OpflowRequestPausingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
