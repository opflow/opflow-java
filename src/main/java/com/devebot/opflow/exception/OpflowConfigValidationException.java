package com.devebot.opflow.exception;

/**
 *
 * @author drupalex
 */
public class OpflowConfigValidationException extends OpflowBootstrapException {

    private Object reason;

    public Object getReason() {
        return reason;
    }

    public OpflowConfigValidationException() {
        super();
    }

    public OpflowConfigValidationException(String message) {
        super(message);
    }

    public OpflowConfigValidationException(String message, Object reason) {
        super(message);
        this.reason = reason;
    }

    public OpflowConfigValidationException(Object reason) {
        super();
        this.reason = reason;
    }
}
