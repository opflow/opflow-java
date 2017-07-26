package net.acegik.jsondataflow;

import com.google.gson.Gson;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowMessage {
    private final static Gson gson = new Gson();
    
    private final byte[] content;
    private final Map<String, Object> info;
    
    public OpflowMessage(byte[] content, Map<String, Object> info) {
        this.content = content;
        this.info = info;
    }

    public byte[] getContent() {
        return content;
    }
    
    public String getContentAsString() {
        return OpflowUtil.getString(content);
    }

    public Map<String, Object> getInfo() {
        return info;
    }
}
