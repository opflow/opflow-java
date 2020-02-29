package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowMessage {

    private final byte[] body;
    private final Map<String, Object> headers;
    
    public final static OpflowMessage EMPTY = new OpflowMessage();
    public final static OpflowMessage ERROR = new OpflowMessage(null, OpflowObjectTree.buildMap(false).put("status", "failed").toMap());
    
    private OpflowMessage() {
        body = null;
        headers = null;
    }
    
    public OpflowMessage(byte[] body, Map<String, Object> headers) {
        this.body = body;
        this.headers = headers;
    }

    public byte[] getBody() {
        return body;
    }
    
    public String getBodyAsString() {
        if (body == null) return null;
        return OpflowUtil.getString(body);
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }
}
