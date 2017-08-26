package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowConsumerOverLimitException extends OpflowOperationException {

    public OpflowConsumerOverLimitException() {
    }

    public OpflowConsumerOverLimitException(String message) {
        super(message);
    }

    public OpflowConsumerOverLimitException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowConsumerOverLimitException(Throwable cause) {
        super(cause);
    }

    public OpflowConsumerOverLimitException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
