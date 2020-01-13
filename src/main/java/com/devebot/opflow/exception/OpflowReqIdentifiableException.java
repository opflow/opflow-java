package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowReqIdentifiableException extends OpflowBootstrapException {

    public OpflowReqIdentifiableException() {
    }

    public OpflowReqIdentifiableException(String message) {
        super(message);
    }

    public OpflowReqIdentifiableException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowReqIdentifiableException(Throwable cause) {
        super(cause);
    }

    public OpflowReqIdentifiableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
