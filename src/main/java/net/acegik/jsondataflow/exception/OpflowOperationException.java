package net.acegik.jsondataflow.exception;

public class OpflowOperationException extends OpflowAbstractException {
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
