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
}
