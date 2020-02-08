package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowUtil;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author pnhung177
 */
public class OpflowObjectTree {

    public interface MapListener extends OpflowUtil.MapListener {
        public void transform(Map<String, Object> opts);
    }
    
    public static class MapBuilder extends OpflowUtil.MapBuilder {
        private final Map<String, Object> fields;

        public MapBuilder() {
            this(null);
        }
        
        public MapBuilder(Map<String, Object> source) {
            fields = ensureNotNull(source);
        }
        
        public MapBuilder put(String key, Object value) {
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
    
    public static MapBuilder buildMap() {
        return buildMap(null, null, false);
    }
    
    public static MapBuilder buildMap(MapListener listener) {
        return buildMap(listener, null, false);
    }
    
    public static MapBuilder buildMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, false);
    }
    
    public static MapBuilder buildMap(MapListener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, false);
    }
    
    public static MapBuilder buildMap(MapListener listener, Map<String, Object> defaultOpts, boolean orderKeep) {
        Map<String, Object> source = orderKeep ? new LinkedHashMap<String, Object>() : new HashMap<String, Object>();
        if (defaultOpts != null) {
            source.putAll(defaultOpts);
        }
        if (listener != null) {
            listener.transform(source);
        }
        return new MapBuilder(source);
    }
    
    public static MapBuilder buildOrderedMap() {
        return buildMap(null, null, true);
    }
    
    public static MapBuilder buildOrderedMap(MapListener listener) {
        return buildMap(listener, null, true);
    }
    
    public static MapBuilder buildOrderedMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, true);
    }
    
    public static MapBuilder buildOrderedMap(MapListener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, true);
    }
}
