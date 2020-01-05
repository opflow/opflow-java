package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRequestWaitingException extends OpflowOperationException {

    public OpflowRequestWaitingException() {
    }

    public OpflowRequestWaitingException(String message) {
        super(message);
    }

    public OpflowRequestWaitingException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRequestWaitingException(Throwable cause) {
        super(cause);
    }

    public OpflowRequestWaitingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
