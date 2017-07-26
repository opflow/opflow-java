package net.acegik.jsondataflow;

import java.io.IOException;

public interface OpflowRpcListener {
    public void processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException;
}
