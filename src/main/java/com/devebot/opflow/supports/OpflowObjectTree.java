package com.devebot.opflow.supports;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author pnhung177
 */
public class OpflowObjectTree {

    public interface Listener<V> {
        public void transform(Map<String, V> opts);
    }
    
    public static class Builder<V> {
        private final Map<String, V> fields;

        public Builder() {
            this(null);
        }
        
        public Builder(Map<String, V> source) {
            fields = (source == null) ? new HashMap<String, V>() : source;
        }
        
        public Builder put(String key, V value) {
            fields.put(key, value);
            return this;
        }

        public Object get(String key) {
            return fields.get(key);
        }

        public Map<String, V> toMap() {
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
    
    public static <V> Builder<V> buildMap() {
        return buildMap(null, null, true);
    }
    
    public static <V> Builder<V> buildMap(Listener<V> listener) {
        return buildMap(listener, null, true);
    }
    
    public static <V> Builder<V> buildMap(Map<String, V> defaultOpts) {
        return buildMap(null, defaultOpts, true);
    }
    
    public static <V> Builder<V> buildMap(Listener<V> listener, Map<String, V> defaultOpts) {
        return buildMap(listener, defaultOpts, true);
    }
    
    public static <V> Builder<V> buildMap(Listener<V> listener, Map<String, V> defaultOpts, boolean orderReserved) {
        Map<String, V> source = orderReserved ? new LinkedHashMap<String, V>() : new HashMap<String, V>();
        if (defaultOpts != null) {
            source.putAll(defaultOpts);
        }
        if (listener != null) {
            listener.transform(source);
        }
        return new Builder(source);
    }
    
    public static <V> Builder<V> buildMap(boolean orderReserved) {
        return buildMap(null, null, orderReserved);
    }
    
    public static <V> Builder<V> buildMap(Listener<V> listener, boolean orderReserved) {
        return buildMap(listener, null, orderReserved);
    }
    
    public static <V> Builder<V> buildMap(Map<String, V> defaultOpts, boolean orderReserved) {
        return buildMap(null, defaultOpts, orderReserved);
    }
    
    public static Map<String, Object> ensureNonNull(Map<String, Object> opts) {
        return (opts == null) ? new HashMap<String, Object>() : opts;
    }
    
    public static Map<String, Object> merge(Map<String, Object> target, Map<String, Object> source) {
        if (target == null) target = new HashMap<>();
        if (source == null) return target;
        for (String key : source.keySet()) {
            if (source.get(key) instanceof Map && target.get(key) instanceof Map) {
                Map<String, Object> targetChild = (Map<String, Object>) target.get(key);
                Map<String, Object> sourceChild = (Map<String, Object>) source.get(key);
                target.put(key, merge(targetChild, sourceChild));
            } else {
                target.put(key, source.get(key));
            }
        }
        return target;
    }
    
    public static <T> T getOptionValue(Map<String, Object> options, String fieldName, Class<T> type, T defval) {
        return getOptionValue(options, fieldName, type, defval, true);
    }

    public static <T> T getOptionValue(Map<String, Object> options, String fieldName, Class<T> type, T defval, boolean assigned) {
        Object value = null;
        if (options != null) value = options.get(fieldName);
        if (value == null) {
            if (assigned) {
                options.put(fieldName, defval);
            }
            return defval;
        } else {
            return OpflowConverter.convert(value, type);
        }
    }
    
    public static Object getObjectByPath(Map<String, Object> options, String[] path) {
        return getObjectByPath(options, path, null);
    }
    
    public static Object getObjectByPath(Map<String, Object> options, String[] path, Object defval) {
        if (options == null) return null;
        if (path == null || path.length == 0) return null;
        Map<String, Object> pointer = options;
        for(int i=0; i<path.length-1; i++) {
            String step = path[i];
            if (step == null || step.isEmpty()) {
                pointer = null;
                break;
            }
            Object value = pointer.get(step);
            if (value instanceof Map) {
                pointer = (Map<String, Object>) value;
            } else {
                pointer = null;
                break;
            }
        }
        if (pointer == null) return null;
        Object value = pointer.get(path[path.length - 1]);
        return (value == null) ? defval : value;
    }
}
