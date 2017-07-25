package net.acegik.jsondataflow;

public class OpflowGeneralException extends RuntimeException {
    public OpflowGeneralException() {
        super();
    }

    public OpflowGeneralException(String message) {
        super(message);
    }

    public OpflowGeneralException(String message, Throwable cause) {
    	super(message, cause);
    }

    public OpflowGeneralException(Throwable cause) {
        super(cause);
    }

    protected OpflowGeneralException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
