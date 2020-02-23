package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowConverter;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowEnvTool;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowNetTool;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.LinkedHashMap;

/**
 *
 * @author drupalex
 */
public class OpflowUtil {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    private static final String INT_RANGE_DELIMITER = "-";
    private static final String INT_RANGE_PATTERN_STRING = "[\\d]{1,}\\s*\\-\\s*[\\d]{1,}";
    
    private static final String INT_ARRAY_DELIMITER = ",";
    private static final String INT_ARRAY_PATTERN_STRING = "[\\d]{1,}\\s*(\\s*,\\s*[\\d]{1,}){0,}";
    
    @Deprecated
    public static String jsonObjectToString(Object jsonObj) {
        return OpflowJsonTool.toString(jsonObj);
    }
    
    @Deprecated
    public static <T> T jsonStringToObject(String json, Class<T> type) {
        return OpflowJsonTool.toObject(json, type);
    }
    
    @Deprecated
    public static String jsonMapToString(Map<String, Object> jsonMap) {
        return OpflowJsonTool.toString(jsonMap);
    }
    
    @Deprecated
    public static Map<String, Object> jsonStringToMap(String json) {
        return OpflowJsonTool.toObjectMap(json);
    }
    
    @Deprecated
    public static <T> T jsonMessageToObject(OpflowMessage message, Class<T> type) {
        return OpflowJsonTool.toObject(message, type);
    }
    
    @Deprecated
    public static Object[] jsonStringToArray(String arrayString, Class[] types) {
        return OpflowJsonTool.toObjectArray(arrayString, types);
    }
    
    @Deprecated
    public static <T> T jsonExtractField(String json, String fieldName, Class<T> type) {
        return OpflowJsonTool.extractField(json, fieldName, type);
    }
    
    @Deprecated
    public static int jsonExtractFieldAsInt(String json, String fieldName) {
        return OpflowJsonTool.extractFieldAsInt(json, fieldName);
    }
    
    @Deprecated
    public static String toISO8601UTC(Date date) {
        return OpflowDateTime.toISO8601UTC(date);
    }
    
    @Deprecated
    public static Date fromISO8601UTC(String dateStr) {
        return OpflowDateTime.fromISO8601UTC(dateStr);
    }
    
    @Deprecated
    public static String getCurrentTimeString() {
        return OpflowDateTime.getCurrentTimeString();
    }
    
    @Deprecated
    public static long getCurrentTime() {
        return OpflowDateTime.getCurrentTime();
    }

    @Deprecated
    public static String getUUID() {
        return OpflowUUID.getUUID();
    }
    
    @Deprecated
    public static String getLogID() {
        return OpflowUUID.getBase64ID();
    }
    
    @Deprecated
    public static String getLogID(String uuid) {
        return OpflowUUID.getBase64ID(uuid);
    }
    
    public static byte[] getBytes(String data) {
        if (data == null) return null;
        try {
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public static String getString(byte[] data) {
        if (data == null) return null;
        try {
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public static String truncate(String source) {
        return truncate(source, 512);
    }
    
    public static String truncate(String source, int limit) {
        if (source == null) return source;
        if (source.length() <= limit) return source;
        return source.substring(0, limit);
    }
    
    public static void copyParameters(Map<String, Object> target, Map<String, Object> source, String[] keys) {
        for(String field: keys) {
            if (source.containsKey(field)) {
                target.put(field, source.get(field));
            }
        }
    }
    
    public static <T> boolean arrayContains(final T[] array, final T v) {
        if (v == null) {
            for (final T e : array) if (e == null) return true;
        } else {
            for (final T e : array) if (e == v || v.equals(e)) return true;
        }
        return false;
    }
    
    public static <T> List<T> mergeLists(List<T> list1, List<T> list2) {
	List<T> list = new ArrayList<>();

	list.addAll(list1);
	list.addAll(list2);

	return list;
    }
    
    @Deprecated
    public interface MapListener {
        public void transform(Map<String, Object> opts);
    }
    
    @Deprecated
    public static class MapBuilder {
        private final Map<String, Object> fields;

        @Deprecated
        public MapBuilder() {
            this(null);
        }
        
        @Deprecated
        public MapBuilder(Map<String, Object> source) {
            fields = ensureNotNull(source);
        }
        
        public MapBuilder put(String key, Object value) {
            fields.put(key, value);
            return this;
        }

        @Deprecated
        public Object get(String key) {
            return fields.get(key);
        }

        @Deprecated
        public Map<String, Object> toMap() {
            return fields;
        }
        
        @Deprecated
        @Override
        public String toString() {
            return toString(false);
        }
        
        @Deprecated
        public String toString(boolean pretty) {
            return OpflowJsonTool.toString(fields, pretty);
        }
    }
    
    @Deprecated
    public static MapBuilder buildMap() {
        return buildMap(null, null, false);
    }
    
    @Deprecated
    public static MapBuilder buildMap(MapListener listener) {
        return buildMap(listener, null, false);
    }
    
    @Deprecated
    public static MapBuilder buildMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, false);
    }
    
    @Deprecated
    public static MapBuilder buildMap(MapListener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, false);
    }
    
    @Deprecated
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
    
    @Deprecated
    public static MapBuilder buildOrderedMap() {
        return buildMap(null, null, true);
    }
    
    @Deprecated
    public static MapBuilder buildOrderedMap(MapListener listener) {
        return buildMap(listener, null, true);
    }
    
    @Deprecated
    public static MapBuilder buildOrderedMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, true);
    }
    
    @Deprecated
    public static MapBuilder buildOrderedMap(MapListener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, true);
    }
    
    public static Map<String, Object> mergeObjectTree(Map<String, Object> target, Map<String, Object> source) {
        if (target == null) target = new HashMap<>();
        if (source == null) return target;
        for (String key : source.keySet()) {
            if (source.get(key) instanceof Map && target.get(key) instanceof Map) {
                Map<String, Object> targetChild = (Map<String, Object>) target.get(key);
                Map<String, Object> sourceChild = (Map<String, Object>) source.get(key);
                target.put(key, mergeObjectTree(targetChild, sourceChild));
            } else {
                target.put(key, source.get(key));
            }
        }
        return target;
    }
    
    public static Map<String, Object> ensureNotNull(Map<String, Object> opts) {
        return (opts == null) ? new HashMap<String, Object>() : opts;
    }
    
    public static String getRequestId(Map<String, Object> headers) {
        return getStringField(headers, CONST.REQUEST_ID, true, true);
    }
    
    public static String getRequestId(Map<String, Object> headers, boolean uuidIfNotFound) {
        return getStringField(headers, CONST.REQUEST_ID, uuidIfNotFound, true);
    }
    
    public static String getRequestTime(Map<String, Object> headers) {
        return getRequestTime(headers, true);
    }
    
    public static String getRequestTime(Map<String, Object> headers, boolean currentIfNotFound) {
        Object date = headers.get(CONST.REQUEST_TIME);
        if (date instanceof String) {
            return (String) date;
        }
        if (date instanceof Date) {
            String requestTime = OpflowDateTime.toISO8601UTC((Date) date);
            headers.put(CONST.REQUEST_TIME, requestTime);
            return requestTime;
        }
        if (date == null && !currentIfNotFound) {
            return null;
        } else {
            String requestTime = OpflowDateTime.getCurrentTimeString();
            headers.put(CONST.REQUEST_TIME, requestTime);
            return requestTime;
        }
    }
    
    public static String[] getRequestTags(Map<String, Object> headers) {
        Object tags = headers.get(CONST.REQUEST_TAGS);
        if (tags == null) {
            return null;
        }
        if (tags instanceof ArrayList) {
            ArrayList tagList = (ArrayList) tags;
            String[] tagArray = new String[tagList.size()];
            for (int i=0; i<tagArray.length; i++) {
                // com.rabbitmq.client.impl.LongStringHelper$ByteArrayLongString
                tagArray[i] = tagList.get(i).toString();
            }
            return tagArray;
        }
        return splitByComma(tags.toString());
    }
    
    public static String getRoutineId(Map<String, Object> headers) {
        return getStringField(headers, CONST.ROUTINE_ID, true, true);
    }
    
    public static String getRoutineId(Map<String, Object> headers, boolean uuidIfNotFound) {
        return getStringField(headers, CONST.ROUTINE_ID, uuidIfNotFound, true);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, boolean uuidIfNotFound, boolean assigned) {
        if (options == null) return null;
        Object value = options.get(fieldName);
        if (value != null) {
            return value.toString();
        }
        String valueStr = uuidIfNotFound ? OpflowUUID.getBase64ID() : null;
        if (assigned) {
            options.put(fieldName, valueStr);
        }
        return valueStr;
    }
    
    public static String getOptionField(Map<String, Object> options, String fieldName, boolean uuidIfNotFound) {
        Object value = getOptionField(options, fieldName, uuidIfNotFound ? OpflowUUID.getBase64ID() : null);
        return value != null ? value.toString() : null;
    }
    
    public static Object getOptionField(Map<String, Object> options, String fieldName, Object defval) {
        Object value = null;
        if (options != null) value = options.get(fieldName);
        return (value == null) ? defval : value;
    }
    
    public static Object getOptionField(Map<String, Object> options, String[] fieldNames) {
        return getOptionField(options, fieldNames, null);
    }
    
    public static Object getOptionField(Map<String, Object> options, String[] fieldNames, Object defval) {
        if (options == null) return null;
        if (fieldNames == null || fieldNames.length == 0) return null;
        Map<String, Object> pointer = options;
        for(int i=0; i<fieldNames.length-1; i++) {
            Object value = pointer.get(fieldNames[i]);
            if (value instanceof Map) {
                pointer = (Map<String, Object>) value;
            } else {
                pointer = null;
                break;
            }
        }
        if (pointer == null) return null;
        Object value = pointer.get(fieldNames[fieldNames.length - 1]);
        return (value == null) ? defval : value;
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

    public static String[] splitByComma(String source) {
        return splitByComma(source, String.class);
    }
    
    public static <T> T[] splitByComma(String source, Class<T> type) {
        return splitByComma(source, type, ",");
    }
    
    public static <T> T[] splitByComma(String source, Class<T> type, String delimiter) {
        if (source == null) return null;
        String[] arr = source.split(delimiter);
        ArrayList<T> list = new ArrayList<>(arr.length);
        for(String item: arr) {
            String str = item.trim();
            if (str.length() > 0) list.add(OpflowConverter.convert(str, type));
        }
        return list.toArray((T[]) Array.newInstance(type, 0));
    }
    
    public static boolean isIntegerArray(String intArray) {
        return intArray != null && intArray.matches(INT_ARRAY_PATTERN_STRING);
    }
    
    public static boolean isIntegerRange(String intRange) {
        return intRange != null && intRange.matches(INT_RANGE_PATTERN_STRING);
    }
    
    public static Integer[] getIntegerRange(String range) {
        try {
            Integer[] minmax = splitByComma(range, Integer.class, INT_RANGE_DELIMITER);
            if (minmax == null) {
                return null;
            }
            if (minmax.length != 2) {
                return new Integer[0];
            }
            if (minmax[0] > minmax[1]) {
                int min = minmax[1];
                minmax[1] = minmax[0];
                minmax[0] = min;
            }
            return minmax;
        }
        catch (Exception e) {
            return null;
        }
    }
    
    public static Integer detectFreePort(Map<String, Object> kwargs, String fieldName, Integer[] defaultPorts) {
        Integer[] ports;
        Object portsObj = OpflowUtil.getOptionField(kwargs, fieldName, null);
        if (portsObj instanceof Integer[]) {
            ports = (Integer[]) portsObj;
            if (ports.length > 0) {
                // ports range from 8989 to 9000: [-1, 8989, 9000]
                if (ports[0] == -1 && ports.length == 3) {
                    return OpflowNetTool.detectFreePort(ports[1], ports[2]);
                }
                return OpflowNetTool.detectFreePort(ports);
            }
        }
        if (portsObj instanceof String) {
            String portsCfg = (String) portsObj;
            ports = OpflowUtil.getIntegerRange(portsCfg);
            if (ports != null && ports.length == 2) {
                return OpflowNetTool.detectFreePort(ports[0], ports[1]);
            }
            ports = OpflowUtil.splitByComma(portsCfg, Integer.class, INT_ARRAY_DELIMITER);
            if (ports != null && ports.length > 0) {
                return OpflowNetTool.detectFreePort(ports);
            }
        }
        return OpflowNetTool.detectFreePort(defaultPorts);
    }
    
    public static String getMessageField(OpflowMessage message, String fieldName) {
        if (message == null || fieldName == null) return null;
        Map<String, Object> info = message.getInfo();
        if (info != null) {
            Object val = info.get(fieldName);
            if (val != null) {
                return val.toString();
            }
        }
        return null;
    }
    
    @Deprecated
    public static String getSystemProperty(String key, String def) {
        return OpflowEnvTool.instance.getSystemProperty(key, def);
    }
    
    @Deprecated
    public static String getEnvironVariable(String key, String def) {
        return OpflowEnvTool.instance.getEnvironVariable(key, def);
    }
    
    public static URL getResource(String location) {
        URL url = null;
        if (url == null) {
            // Attempt to load resource from the context class path of current thread
            // may throw the SecurityException
            try {
                url = Thread.currentThread().getContextClassLoader().getResource(location);
            } catch(Exception ex) {}
        }
        if (url == null) {
            // Last attempt: get the resource from the class path.
            try {
                url = ClassLoader.getSystemResource(location);
            } catch(Exception ex) {}
        }
        return url;
    }
    
    private static Pattern GENERIC_PATTERN = Pattern.compile("<.*>");
    
    public static boolean isGenericDeclaration(String signature) {
        return GENERIC_PATTERN.matcher(signature).find();
    }
    
    private final static String OBJECT_CLASS_NAME = Object.class.getCanonicalName();
    
    public static List<Class<?>> getAllAncestorTypes(Class<?> clazz) {
        List<Class<?>> bag = new ArrayList<>();
        if (clazz == null) return bag;
        do {
            bag.add(clazz);
            // Add all the interfaces implemented by this class
            Class<?>[] interfaces = clazz.getInterfaces();
            if (interfaces.length > 0) {
                bag.addAll(Arrays.asList(interfaces));
                // inspect the ancestors of interfaces
                for (Class<?> interfaze : interfaces) {
                    bag.addAll(getAllAncestorTypes(interfaze));
                }
            }
            // Add the super class
            Class<?> superClass = clazz.getSuperclass();
            // Interfaces does not have superclass, so break and return
            if (superClass == null) break;
            // Now inspect the superclass recursively
            clazz = superClass;
        } while (!OBJECT_CLASS_NAME.equals(clazz.getCanonicalName()));
        bag = new ArrayList<>(new HashSet<>(bag));
        Collections.reverse(bag);
        return bag;
    }
    
    public static String getMethodSignature(Method method) {
        return method.toString();
    }
    
    public static String maskPassword(String password) {
        if (password == null) return null;
        char[] charArray = new char[password.length()];
        Arrays.fill(charArray, '*');
        return new String(charArray);
    }
    
    private static Pattern passwordPattern = Pattern.compile(":([^:]+)@");
    
    public static String hidePasswordInUri(String uri) {
        return passwordPattern.matcher(uri).replaceAll(":******@");
    }
    
    public static boolean isComponentEnabled(Map<String, Object> cfg) {
        return cfg != null && !Boolean.FALSE.equals(cfg.get("enabled"));
    }
    
    public static boolean isAMQPEntrypointNull(Map<String, Object> cfg) {
        return cfg.get("exchangeName") == null || cfg.get("routingKey") == null;
    }
    
    public static String getAMQPEntrypointCode(Map<String, Object> cfg) {
        return cfg.get("exchangeName").toString() + cfg.get("routingKey").toString();
    }
    
    public static String getClassSimpleName(Class clazz) {
        return clazz.getSimpleName();
    }
    
    public static String extractClassName(Class clazz) {
        return clazz.getName().replace(clazz.getPackage().getName(), "");
    }
    
    public static void sleep(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException ex) {}
    }
}
