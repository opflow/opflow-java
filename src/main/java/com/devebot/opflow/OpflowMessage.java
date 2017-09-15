package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowMessage {

    private final byte[] body;
    private final Map<String, Object> info;
    
    public final static OpflowMessage EMPTY = new OpflowMessage();
    public final static OpflowMessage ERROR = new OpflowMessage();
    
    private OpflowMessage() {
        body = null;
        info = null;
    }
    
    public OpflowMessage(byte[] body, Map<String, Object> info) {
        this.body = body;
        this.info = info;
    }

    public byte[] getBody() {
        return body;
    }
    
    public String getBodyAsString() {
        if (body == null) return null;
        return OpflowUtil.getString(body);
    }

    public Map<String, Object> getInfo() {
        return info;
    }
}
