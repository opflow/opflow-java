package net.acegik.jsondataflow;

import java.io.IOException;

public interface OpflowPubsubListener {
    public void processMessage(OpflowMessage message) throws IOException;
}
