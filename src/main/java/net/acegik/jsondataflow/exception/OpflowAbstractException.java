package net.acegik.jsondataflow.exception;

public class OpflowAbstractException extends RuntimeException {
    public OpflowAbstractException() {
        super();
    }

    public OpflowAbstractException(String message) {
        super(message);
    }

    public OpflowAbstractException(String message, Throwable cause) {
    	super(message, cause);
    }

    public OpflowAbstractException(Throwable cause) {
        super(cause);
    }

    protected OpflowAbstractException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
