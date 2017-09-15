package com.devebot.opflow;

import com.google.gson.Gson;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowLogTracer {
    private final static String INSTANCE_ID = OpflowUtil.getUUID();
    private final static Gson GSON = new Gson();
    
    private final Map<String, Object> fields = new LinkedHashMap<String, Object>();
    
    public OpflowLogTracer() {
        fields.put("message", null);
        fields.put("instanceId", INSTANCE_ID);
    }
    
    public OpflowLogTracer copy() {
        OpflowLogTracer target = new OpflowLogTracer();
        target.fields.putAll(fields);
        return target;
    }
    
    public OpflowLogTracer copy(String[] copied) {
        OpflowLogTracer target = copy();
        for(String key: target.fields.keySet()) {
            if (!OpflowUtil.arrayContains(copied, key)) {
                target.fields.remove(key);
            }
        }
        return target;
    }
    
    public OpflowLogTracer put(String key, String value) {
        fields.put(key, value);
        return this;
    }
    
    public Object get(String key) {
        return fields.get(key);
    }
    
    @Override
    public String toString() {
        return GSON.toJson(fields);
    }
}
