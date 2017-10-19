package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowFailedConversionException extends OpflowOperationException {

    public OpflowFailedConversionException() {
    }

    public OpflowFailedConversionException(String message) {
        super(message);
    }

    public OpflowFailedConversionException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowFailedConversionException(Throwable cause) {
        super(cause);
    }

    public OpflowFailedConversionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
