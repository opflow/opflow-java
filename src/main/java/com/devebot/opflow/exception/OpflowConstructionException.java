package com.devebot.opflow.exception;

public class OpflowConstructionException extends RuntimeException {
    public OpflowConstructionException() {
        super();
    }

    public OpflowConstructionException(String message) {
        super(message);
    }

    public OpflowConstructionException(String message, Throwable cause) {
    	super(message, cause);
    }

    public OpflowConstructionException(Throwable cause) {
        super(cause);
    }

    protected OpflowConstructionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
