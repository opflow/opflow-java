package com.devebot.opflow.supports;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author pnhung177
 */
public class OpflowObjectTree {

    public interface Listener {
        public void transform(Map<String, Object> opts);
    }
    
    public static class Builder {
        private final Map<String, Object> fields;

        public Builder() {
            this(null);
        }
        
        public Builder(Map<String, Object> source) {
            fields = ensureNotNull(source);
        }
        
        public Builder put(String key, Object value) {
            fields.put(key, value);
            return this;
        }

        public Object get(String key) {
            return fields.get(key);
        }

        public Map<String, Object> toMap() {
            return fields;
        }
        
        @Override
        public String toString() {
            return toString(false);
        }
        
        public String toString(boolean pretty) {
            return OpflowJsonTool.toString(fields, pretty);
        }
    }
    
    public static Map<String, Object> ensureNotNull(Map<String, Object> opts) {
        return (opts == null) ? new HashMap<String, Object>() : opts;
    }
    
    public static Builder buildMap() {
        return buildMap(null, null, false);
    }
    
    public static Builder buildMap(Listener listener) {
        return buildMap(listener, null, false);
    }
    
    public static Builder buildMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, false);
    }
    
    public static Builder buildMap(Listener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, false);
    }
    
    public static Builder buildMap(Listener listener, Map<String, Object> defaultOpts, boolean orderReserved) {
        Map<String, Object> source = orderReserved ? new LinkedHashMap<String, Object>() : new HashMap<String, Object>();
        if (defaultOpts != null) {
            source.putAll(defaultOpts);
        }
        if (listener != null) {
            listener.transform(source);
        }
        return new Builder(source);
    }
    
    public static Builder buildOrderedMap() {
        return buildMap(null, null, true);
    }
    
    public static Builder buildOrderedMap(Listener listener) {
        return buildMap(listener, null, true);
    }
    
    public static Builder buildOrderedMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, true);
    }
    
    public static Builder buildOrderedMap(Listener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, true);
    }
}
