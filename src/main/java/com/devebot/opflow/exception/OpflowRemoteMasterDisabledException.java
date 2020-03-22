package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRemoteMasterDisabledException extends OpflowBootstrapException {

    public OpflowRemoteMasterDisabledException() {
    }

    public OpflowRemoteMasterDisabledException(String message) {
        super(message);
    }

    public OpflowRemoteMasterDisabledException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRemoteMasterDisabledException(Throwable cause) {
        super(cause);
    }

    public OpflowRemoteMasterDisabledException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
