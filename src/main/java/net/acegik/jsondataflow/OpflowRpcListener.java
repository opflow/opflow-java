package net.acegik.jsondataflow;

import java.io.IOException;
import java.util.Map;

public interface OpflowRpcListener {
    public void processMessage(byte[] content, Map<String, Object> info, OpflowRpcResponse feedback) throws IOException;
}
