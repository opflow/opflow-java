package com.devebot.opflow;

import java.io.IOException;

public interface OpflowPubsubListener {
    public void processMessage(OpflowEngine.Message message) throws IOException;
}
