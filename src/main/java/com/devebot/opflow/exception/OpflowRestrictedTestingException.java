package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRestrictedTestingException extends OpflowOperationException {

    public OpflowRestrictedTestingException() {
    }

    public OpflowRestrictedTestingException(String message) {
        super(message);
    }

    public OpflowRestrictedTestingException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRestrictedTestingException(Throwable cause) {
        super(cause);
    }

    public OpflowRestrictedTestingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
