package net.acegik.jsondataflow;

import java.io.IOException;

public interface OpflowRpcListener {
    public static final Boolean DONE = Boolean.FALSE;
    public static final Boolean NEXT = Boolean.TRUE;
    public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException;
}
