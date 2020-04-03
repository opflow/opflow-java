package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowNonOperatingException extends OpflowOperationException {

    public OpflowNonOperatingException() {
    }

    public OpflowNonOperatingException(String message) {
        super(message);
    }

    public OpflowNonOperatingException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowNonOperatingException(Throwable cause) {
        super(cause);
    }

    public OpflowNonOperatingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
