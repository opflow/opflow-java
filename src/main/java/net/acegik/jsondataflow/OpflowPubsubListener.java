package net.acegik.jsondataflow;

import java.io.IOException;
import java.util.Map;

public interface OpflowPubsubListener {
    public void processMessage(byte[] content, Map<String, Object> info) throws IOException;
}
