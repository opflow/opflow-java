package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRpcMasterDisabledException extends OpflowOperationException {

    public OpflowRpcMasterDisabledException() {
    }

    public OpflowRpcMasterDisabledException(String message) {
        super(message);
    }

    public OpflowRpcMasterDisabledException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRpcMasterDisabledException(Throwable cause) {
        super(cause);
    }

    public OpflowRpcMasterDisabledException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
