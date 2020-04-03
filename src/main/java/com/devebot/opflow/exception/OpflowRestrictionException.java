package com.devebot.opflow.exception;

public class OpflowRestrictionException extends RuntimeException {
    public OpflowRestrictionException() {
        super();
    }

    public OpflowRestrictionException(String message) {
        super(message);
    }

    public OpflowRestrictionException(String message, Throwable cause) {
    	super(message, cause);
    }

    public OpflowRestrictionException(Throwable cause) {
        super(cause);
    }

    public OpflowRestrictionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
