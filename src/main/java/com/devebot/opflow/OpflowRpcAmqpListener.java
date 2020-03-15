package com.devebot.opflow;

import java.io.IOException;

@Deprecated
public interface OpflowRpcAmqpListener extends OpflowRpcAmqpWorker.Listener {
    @Override
    public Boolean processMessage(OpflowMessage message, OpflowRpcAmqpResponse response) throws IOException;
}
