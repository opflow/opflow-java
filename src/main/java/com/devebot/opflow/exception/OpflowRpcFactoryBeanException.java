package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowRpcFactoryBeanException extends OpflowOperationException {

    public OpflowRpcFactoryBeanException() {
    }

    public OpflowRpcFactoryBeanException(String message) {
        super(message);
    }

    public OpflowRpcFactoryBeanException(String message, Throwable cause) {
        super(message, cause);
    }

    public OpflowRpcFactoryBeanException(Throwable cause) {
        super(cause);
    }

    public OpflowRpcFactoryBeanException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
