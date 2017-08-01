package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowConsumerLimitExceedException extends OpflowOperationException {

    public OpflowConsumerLimitExceedException() {
    }

    public OpflowConsumerLimitExceedException(String message) {
        super(message);
    }

    public OpflowConsumerLimitExceedException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowConsumerLimitExceedException(Throwable cause) {
        super(cause);
    }

    public OpflowConsumerLimitExceedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
