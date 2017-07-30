package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowMessage {

    private final byte[] content;
    private final Map<String, Object> info;
    
    public final static OpflowMessage EMPTY = new OpflowMessage();
    public final static OpflowMessage ERROR = new OpflowMessage();
    
    private OpflowMessage() {
        content = null;
        info = null;
    }
    
    public OpflowMessage(byte[] content, Map<String, Object> info) {
        this.content = content;
        this.info = info;
    }

    public byte[] getContent() {
        return content;
    }
    
    public String getContentAsString() {
        if (content == null) return null;
        return OpflowUtil.getString(content);
    }

    public Map<String, Object> getInfo() {
        return info;
    }
}
