package com.devebot.opflow;

import java.io.IOException;

public interface OpflowRpcAmqpListener {
    public static final Boolean DONE = Boolean.FALSE;
    public static final Boolean NEXT = Boolean.TRUE;
    public Boolean processMessage(OpflowMessage message, OpflowRpcAmqpResponse response) throws IOException;
}
