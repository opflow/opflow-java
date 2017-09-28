package com.devebot.opflow;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Date;
import javax.xml.bind.DatatypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.exception.OpflowJsonTransformationException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.google.gson.JsonArray;

/**
 *
 * @author drupalex
 */
public class OpflowUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowUtil.class);
    private final OpflowLogTracer logTracer = OpflowLogTracer.ROOT.copy();
    
    private static final Gson GSON = new Gson();
    private static final JsonParser JSON_PARSER = new JsonParser();
    
    private final static boolean OPFLOW_BASE64UUID;
    static {
        OPFLOW_BASE64UUID = !"false".equals(OpflowUtil.getSystemProperty("OPFLOW_BASE64UUID", null)) &&
                !"false".equals(OpflowUtil.getEnvironVariable("OPFLOW_BASE64UUID", null));
    }
    
    public static String jsonObjectToString(Object jsonObj) {
        return GSON.toJson(jsonObj);
    }
    
    public static <T> T jsonStringToObject(String json, Class<T> type) {
        return GSON.fromJson(json, type);
    }
    
    public static String jsonMapToString(Map<String, Object> jsonMap) {
        return GSON.toJson(jsonMap);
    }
    
    public static Map<String, Object> jsonStringToMap(String json) {
        try {
            Map<String,Object> map = GSON.fromJson(json, Map.class);
            return map;
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static <T> T jsonMessageToObject(OpflowMessage message, Class<T> type) {
        return GSON.fromJson(message.getBodyAsString(), type);
    }
    
    public static Object[] jsonStringToArray(String arrayString, Class[] types) {
        if (arrayString == null) return new Object[0];
        JsonArray array = JSON_PARSER.parse(arrayString).getAsJsonArray();
        Object[] args = new Object[types.length];
        for(int i=0; i<types.length; i++) {
            args[i] = GSON.fromJson(array.get(i), types[i]);
        }
        return args;
    }
    
    public static <T> T jsonExtractField(String json, String fieldName, Class<T> type) {
        try {
            JsonObject jsonObject = (JsonObject)JSON_PARSER.parse(json);
            return type.cast(jsonObject.get(fieldName));
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static int jsonExtractFieldAsInt(String json, String fieldName) {
        try {
            JsonObject jsonObject = (JsonObject)JSON_PARSER.parse(json);
            return jsonObject.get(fieldName).getAsInt();
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
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
            return GSON.toJson(fields);
        }
    }
    
    public static MapBuilder buildMap() {
        return buildMap(null);
    }
    
    public static MapBuilder buildMap(MapListener listener) {
        return buildMap(listener, null);
    }
    
    public static MapBuilder buildMap(MapListener listener, Map<String, Object> defaultOpts) {
        Map<String, Object> source = new HashMap<String, Object>();
        if (defaultOpts != null) {
            source.putAll(defaultOpts);
        }
        if (listener != null) {
            listener.transform(source);
        }
        return new MapBuilder(source);
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
    
    public static String[] splitByComma(String source) {
        if (source == null) return null;
        String[] arr = source.split(",");
        ArrayList<String> list = new ArrayList<String>(arr.length);
        for(String item: arr) {
            String str = item.trim();
            if (str.length() > 0) list.add(str);
        }
        return list.toArray(new String[0]);
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
        try {
            return System.getProperty(key, def);
        } catch (Throwable t) {
            if (LOG.isInfoEnabled()) LOG.info("Was not allowed to read system property [" + key + "].");
            return def;
        }
    }
    
    public static String getEnvironVariable(String key, String def) {
        if (key == null) return null;
        try {
            String value = System.getenv(key);
            if (value != null) return value;
            return def;
        } catch (Throwable t) {
            if (LOG.isInfoEnabled()) LOG.info("Was not allowed to read environment variable [" + key + "].");
            return def;
        }
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
}
