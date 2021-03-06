package com.devebot.opflow.supports;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
            fields = (source == null) ? new HashMap<>() : source;
        }
        
        public Builder put(String key, V value) {
            return put(key, value, true);
        }
        
        public Builder put(String key, V value, boolean when) {
            if (when) {
                fields.put(key, value);
            }
            return this;
        }
        
        public Builder add(Map<String, V> source) {
            if (source != null) {
                fields.putAll(source);
            }
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
        Map<String, V> source = orderReserved ? new LinkedHashMap<>() : new HashMap<>();
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
        return (opts == null) ? new HashMap<>() : opts;
    }
    
    public static Map<String, Object> assertChildMap(Map<String, Object> map, String childName) {
        Map<String, Object> childMap;
        Object childObj = map.get(childName);
        if (childObj instanceof Map) {
            childMap = (Map<String, Object>) childObj;
        } else {
            childMap = new LinkedHashMap<>();
            map.put(childName, childMap);
        }
        return childMap;
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
        return traverseMapByPath(options, path);
    }
    
    public static Object getObjectByPath(Map<String, Object> options, String[] path, Object defval) {
        if (options == null || path == null || path.length == 0) return null;
        Object value = traverseMapByPath(options, path);
        return (value == null) ? defval : value;
    }
    
    private static Object traverseMapByPath(Map<String, Object> source, String[] path) {
        if (path == null || path.length == 0) return null;
        Map<String, Object> pointer = source;
        Object value = null;
        for(String step : path) {
            if (step == null || step.isEmpty()) pointer = null;
            if (pointer == null) return null;
            value = pointer.get(step);
            pointer = (value instanceof Map) ? (Map<String, Object>) value : null;
        }
        return value;
    }
    
    public static Map<String, Object> getChildMapByPath(Map<String, Object> source, String[] path) {
        if (path == null || path.length == 0) {
            return source;
        }
        Object sourceObject = traverseMapByPath(source, path);
        if(sourceObject != null && sourceObject instanceof Map) {
            return (Map<String, Object>) sourceObject;
        }
        return null;
    }
    
    public static void renameChildMapField(Map<String, Object> source, String[] path, String oldName, String newName) {
        renameChildMapField(source, path, oldName, newName, true);
    }
    
    public static void renameChildMapField(Map<String, Object> source, String[] path, String oldName, String newName, boolean overridden) {
        if (oldName == null || oldName.isEmpty()) return;
        if (newName == null || newName.isEmpty()) return;
        if (oldName.equals(newName)) return;
        Map<String, Object> parent = getChildMapByPath(source, path);
        if (parent != null) {
            if (parent.containsKey(oldName)) {
                if (overridden || !parent.containsKey(newName)) {
                    parent.put(newName, parent.remove(oldName));
                }
            }
        }
    }
    
    public static final String[] EMPTY_ARRAY = new String[0];
    
    public interface LeafUpdater {
        public Object transform(String[] path, Object value);
    }
    
    public static Map<String, Object> traverseTree(Map<String, Object> tree, LeafUpdater updater) {
        return traverseTree(new LinkedList<>(), tree, updater);
    }
    
    private static Map<String, Object> traverseTree(LinkedList<String> path, Map<String, Object> tree, LeafUpdater updater) {
        if (tree == null) return null;

        for (String name : tree.keySet()) {
            path.addLast(name);
            Object item = tree.get(name);
            if (item instanceof Map) {
                traverseTree(path, (Map<String, Object>) item, updater);
            } else {
                Object result = updater.transform(path.toArray(EMPTY_ARRAY), item);
                tree.put(name, result);
            }
            path.removeLast();
        }
        
        return tree;
    }
}
