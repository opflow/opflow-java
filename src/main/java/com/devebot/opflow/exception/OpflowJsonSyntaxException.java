package com.devebot.opflow.exception;

import com.google.gson.nostro.JsonSyntaxException;

/**
 *
 * @author drupalex
 */
public class OpflowJsonSyntaxException extends OpflowJsonTransformationException {

    public OpflowJsonSyntaxException() {
    }

    public OpflowJsonSyntaxException(String message) {
        super(message);
    }

    public OpflowJsonSyntaxException(String message, JsonSyntaxException cause) {
        super(message, cause);
    }

    public OpflowJsonSyntaxException(JsonSyntaxException cause) {
        super(cause);
    }
}
