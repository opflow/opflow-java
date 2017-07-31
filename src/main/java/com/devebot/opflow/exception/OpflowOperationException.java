package com.devebot.opflow.exception;

public class OpflowOperationException extends RuntimeException {
    public OpflowOperationException() {
        super();
    }

    public OpflowOperationException(String message) {
        super(message);
    }

    public OpflowOperationException(String message, Throwable cause) {
    	super(message, cause);
    }

    public OpflowOperationException(Throwable cause) {
        super(cause);
    }

    protected OpflowOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
