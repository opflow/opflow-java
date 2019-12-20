package com.devebot.opflow;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowConverter;
import com.devebot.opflow.supports.OpflowEnvtool;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.TimeZone;

/**
 *
 * @author drupalex
 */
public class OpflowUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowUtil.class);
    private static final String ISO8601_TEMPLATE = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat(ISO8601_TEMPLATE);
    private static final boolean OPFLOW_BASE64UUID;
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        OPFLOW_BASE64UUID = !"false".equals(OpflowUtil.getSystemProperty("OPFLOW_BASE64UUID", null)) &&
                !"false".equals(OpflowUtil.getEnvironVariable("OPFLOW_BASE64UUID", null));
    }
    
    @Deprecated
    public static String jsonObjectToString(Object jsonObj) {
        return OpflowJsontool.toString(jsonObj);
    }
    
    @Deprecated
    public static <T> T jsonStringToObject(String json, Class<T> type) {
        return OpflowJsontool.toObject(json, type);
    }
    
    @Deprecated
    public static String jsonMapToString(Map<String, Object> jsonMap) {
        return OpflowJsontool.toString(jsonMap);
    }
    
    @Deprecated
    public static Map<String, Object> jsonStringToMap(String json) {
        return OpflowJsontool.toObjectMap(json);
    }
    
    @Deprecated
    public static <T> T jsonMessageToObject(OpflowMessage message, Class<T> type) {
        return OpflowJsontool.toObject(message, type);
    }
    
    @Deprecated
    public static Object[] jsonStringToArray(String arrayString, Class[] types) {
        return OpflowJsontool.toObjectArray(arrayString, types);
    }
    
    @Deprecated
    public static <T> T jsonExtractField(String json, String fieldName, Class<T> type) {
        return OpflowJsontool.extractField(json, fieldName, type);
    }
    
    @Deprecated
    public static int jsonExtractFieldAsInt(String json, String fieldName) {
        return OpflowJsontool.extractFieldAsInt(json, fieldName);
    }
    
    public static String toISO8601UTC(Date date) {
        return DATE_FORMAT.format(date);
    }
    
    public static Date fromISO8601UTC(String dateStr) {
        try {
            return DATE_FORMAT.parse(dateStr);
        } catch (ParseException e) {}
        return null;
    }
    
    public static String getCurrentTimeString() {
        return toISO8601UTC(new Date());
    }
    
    public static long getCurrentTime() {
        return (new Date()).getTime();
    }
    
    public static String getUUID() {
        return UUID.randomUUID().toString();
    }
    
    public static String getLogID() {
        if (!OPFLOW_BASE64UUID) getUUID();
        return convertUUIDToBase64(UUID.randomUUID());
    }
    
    public static String getLogID(String uuid) {
        if (!OPFLOW_BASE64UUID) return uuid;
        if (uuid == null) return getLogID();
        return convertUUIDToBase64(UUID.fromString(uuid));
    }
    
    private static String convertUUIDToBase64(UUID uuid) {
        // Create byte[] for base64 from uuid
        byte[] src = ByteBuffer.wrap(new byte[16])
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        // Encode to Base64 and remove trailing ==
        return DatatypeConverter.printBase64Binary(src).substring(0, 22);
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
            target.put(field, source.get(field));
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
    
    public interface MapListener {
        public void transform(Map<String, Object> opts);
    }
    
    public static class MapBuilder {
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
            return OpflowJsontool.toString(fields, pretty);
        }
    }
    
    public static MapBuilder buildMap() {
        return buildMap(null, null);
    }
    
    public static MapBuilder buildMap(MapListener listener) {
        return buildMap(listener, null);
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
        return buildOrderedMap(null, null);
    }
    
    public static MapBuilder buildOrderedMap(MapListener listener) {
        return buildOrderedMap(listener, null);
    }
    
    public static MapBuilder buildOrderedMap(Map<String, Object> defaultOpts) {
        return buildMap(null, defaultOpts, true);
    }
    
    public static MapBuilder buildOrderedMap(MapListener listener, Map<String, Object> defaultOpts) {
        return buildMap(listener, defaultOpts, true);
    }
    
    public static Map<String, Object> ensureNotNull(Map<String, Object> opts) {
        return (opts == null) ? new HashMap<String, Object>() : opts;
    }
    
    public static String getRequestId(Map<String, Object> headers) {
        return getRequestId(headers, true);
    }
    
    public static String getRequestId(Map<String, Object> headers, boolean uuidIfNotFound) {
        return getOptionField(headers, "requestId", uuidIfNotFound);
    }
    
    public static String getRoutineId(Map<String, Object> headers) {
        return getRoutineId(headers, true);
    }
    
    public static String getRoutineId(Map<String, Object> headers, boolean uuidIfNotFound) {
        return getOptionField(headers, "routineId", uuidIfNotFound);
    }
    
    public static String getOptionField(Map<String, Object> options, String fieldName, boolean uuidIfNotFound) {
        Object value = getOptionField(options, fieldName, uuidIfNotFound ? getLogID() : null);
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
    
    public static String[] splitByComma(String source) {
        if (source == null) return null;
        String[] arr = source.split(",");
        ArrayList<String> list = new ArrayList<>(arr.length);
        for(String item: arr) {
            String str = item.trim();
            if (str.length() > 0) list.add(str);
        }
        return list.toArray(new String[0]);
    }
    
    public static <T> T[] splitByComma(String source, Class<T> type) {
        if (source == null) return null;
        String[] arr = source.split(",");
        ArrayList<T> list = new ArrayList<>(arr.length);
        for(String item: arr) {
            String str = item.trim();
            if (str.length() > 0) list.add(OpflowConverter.convert(str, type));
        }
        return list.toArray((T[]) Array.newInstance(type, 0));
    }
    
    public static String getMessageField(OpflowMessage message, String fieldName) {
        if (message == null || fieldName == null) return null;
        Map<String, Object> info = message.getInfo();
        if (info != null && info.get(fieldName) != null) {
            return info.get(fieldName).toString();
        }
        return null;
    }
    
    public static String getSystemProperty(String key, String def) {
        return OpflowEnvtool.instance.getSystemProperty(key, def);
    }
    
    public static String getEnvironVariable(String key, String def) {
        return OpflowEnvtool.instance.getEnvironVariable(key, def);
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
    
    public static List<Class<?>> getAllAncestorTypes(Class<?> clazz) {
        List<Class<?>> bag = new ArrayList<Class<?>>();
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
        } while (!"java.lang.Object".equals(clazz.getCanonicalName()));
        bag = new ArrayList<Class<?>>(new HashSet<Class<?>>(bag));
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
}
