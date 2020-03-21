package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowEnvTool;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowNetTool;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowStringUtil;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
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
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    
    private final static boolean IS_PING_LOGGING_OMITTED;
    
    static {
        IS_PING_LOGGING_OMITTED = !"false".equals(ENVTOOL.getEnvironVariable("OPFLOW_OMIT_PING_LOGS", null));
    }
    
    public static class OmitInternalOplogs implements OpflowLogTracer.Customizer {
        final boolean isInternalOplog;
        
        public OmitInternalOplogs(boolean internal) {
            this.isInternalOplog = internal;
        }
        
        public OmitInternalOplogs(String routineScope) {
            this("internal".equals(routineScope));
        }
        
        public OmitInternalOplogs(Map<String, Object> options) {
            this(getOptionField(options, CONST.AMQP_HEADER_ROUTINE_SCOPE, false));
        }
        
        @Override
        public boolean isMute() {
            return IS_PING_LOGGING_OMITTED && isInternalOplog;
        }
    }
    
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
    
    @Deprecated
    public interface MapListener {
        public void transform(Map<String, Object> opts);
    }
    
    @Deprecated
    public static class MapBuilder {
        private final Map<String, Object> fields;

        public MapBuilder() {
            this(null);
        }
        
        public MapBuilder(Map<String, Object> source) {
            fields = OpflowObjectTree.ensureNonNull(source);
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
    
    public static String getRoutineId(Map<String, Object> headers) {
        return getRoutineId(headers, true);
    }
    
    public static String getRoutineId(Map<String, Object> headers, boolean uuidIfNotFound) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            String val = getStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_ID, false, false);
            if (val != null) {
                return val;
            }
        }
        return getStringField(headers, CONST.AMQP_HEADER_ROUTINE_ID, uuidIfNotFound, true);
    }
    
    public static void setRoutineId(Map<String, Object> headers, String value) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            setStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_ID, value);
        }
        setStringField(headers, CONST.AMQP_HEADER_ROUTINE_ID, value);
    }
    
    public static String getRoutineTimestamp(Map<String, Object> headers) {
        return getRoutineTimestamp(headers, true);
    }
    
    public static String getRoutineTimestamp(Map<String, Object> headers, boolean currentIfNotFound) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            String val = getDateField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_TIMESTAMP, false);
            if (val != null) {
                return val;
            }
        }
        return getDateField(headers, CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, currentIfNotFound);
    }
    
    public static void setRoutineTimestamp(Map<String, Object> headers, String value) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            setStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_TIMESTAMP, value);
        }
        setStringField(headers, CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, value);
    }
    
    public static String getRoutineSignature(Map<String, Object> headers) {
        return getRoutineSignature(headers, true);
    }
    
    public static String getRoutineSignature(Map<String, Object> headers, boolean uuidIfNotFound) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            String val = getStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_SIGNATURE, false, false);
            if (val != null) {
                return val;
            }
        }
        return getStringField(headers, CONST.AMQP_HEADER_ROUTINE_SIGNATURE, uuidIfNotFound, true);
    }
    
    public static void setRoutineSignature(Map<String, Object> headers, String value) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            setStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_SIGNATURE, value);
        }
        setStringField(headers, CONST.AMQP_HEADER_ROUTINE_SIGNATURE, value);
    }
    
    public static String getRoutineScope(Map<String, Object> headers) {
        if (CONST.LEGACY_HEADER_APPLIED) {
            String val = getStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_SCOPE, false, false);
            if (val != null) {
                return val;
            }
        }
        return getStringField(headers, CONST.AMQP_HEADER_ROUTINE_SCOPE, false, false);
    }
    
    public static void setRoutineScope(Map<String, Object> headers, String value) {
        if (value != null) {
            setStringField(headers, CONST.AMQP_HEADER_ROUTINE_SCOPE, value);
            if (CONST.LEGACY_HEADER_APPLIED) {
                setStringField(headers, OpflowConstant.LEGACY_HEADER_ROUTINE_SCOPE, value);
            }
        }
    }
    
    public static String[] getRoutineTags(Map<String, Object> headers) {
        return getRoutineTags(headers, CONST.AMQP_HEADER_ROUTINE_TAGS);
    }
    
    public static String[] getRoutineTags(Map<String, Object> headers, String headerName) {
        Object tags = headers.get(headerName);
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
        return OpflowStringUtil.splitByComma(tags.toString());
    }
    
    public static void setRoutineTags(Map<String, Object> headers, String[] tags) {
        if (headers == null) return;
        if (tags != null) {
            headers.put(CONST.AMQP_HEADER_ROUTINE_TAGS, tags);
        }
    }
    
    public static Boolean getProgressEnabled(Map<String, Object> headers) {
        if (headers.get(CONST.AMQP_HEADER_PROGRESS_ENABLED) instanceof Boolean) {
            return (Boolean) headers.get(CONST.AMQP_HEADER_PROGRESS_ENABLED);
        }
        return null;
    }
    
    public static void setProgressEnabled(Map<String, Object> headers, Boolean value) {
        if (value != null) {
            headers.put(CONST.AMQP_HEADER_PROGRESS_ENABLED, value);
        }
    }
    
    public static String getDateField(Map<String, Object> headers, String fieldName, boolean currentIfNotFound) {
        Object date = headers.get(fieldName);
        if (date instanceof String) {
            return (String) date;
        }
        if (date instanceof Date) {
            String routineTimestamp = OpflowDateTime.toISO8601UTC((Date) date);
            headers.put(fieldName, routineTimestamp);
            return routineTimestamp;
        }
        if (date != null) {
            return date.toString();
        } else {
            if (currentIfNotFound) {
                String routineTimestamp = OpflowDateTime.getCurrentTimeString();
                headers.put(fieldName, routineTimestamp);
                return routineTimestamp;
            }
            return null;
        }
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
    
    public static void setStringField(Map<String, Object> options, String fieldName, String value) {
        if (options == null) return;
        options.put(fieldName, value);
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
    
    public static Map<String, Object> getChildMap(Map<String, Object> kwargs, String fieldName) {
        if (kwargs == null) {
            return null;
        }
        if (fieldName == null) {
            return kwargs;
        }
        Object childObject = kwargs.get(fieldName);
        if (childObject instanceof Map) {
            return (Map<String, Object>) childObject;
        }
        return null;
    }
    
    public static Map<String, Object> getChildMap(Map<String, Object> kwargs, String ... fieldNames) {
        if (kwargs == null) {
            return null;
        }
        if (fieldNames == null) {
            return kwargs;
        }
        for (String fieldName : fieldNames) {
            Map<String, Object> childMap = getChildMap(kwargs, fieldName);
            if (childMap != null) {
                return childMap;
            }
        }
        return null;
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
            ports = OpflowStringUtil.getIntegerRange(portsCfg);
            if (ports != null && ports.length == 2) {
                return OpflowNetTool.detectFreePort(ports[0], ports[1]);
            }
            ports = OpflowStringUtil.splitIntegerArray(portsCfg);
            if (ports != null && ports.length > 0) {
                return OpflowNetTool.detectFreePort(ports);
            }
        }
        return OpflowNetTool.detectFreePort(defaultPorts);
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
    
    public static String getMethodSignature(Class clazz, String methodName, Class ... args) {
        try {
            Method method = clazz.getMethod(methodName, args);
            return method.toString();
        } catch (NoSuchMethodException | SecurityException ex) {
            return null;
        }
    }
    
    public static String getMethodSignature(Method method) {
        return method.toString();
    }
    
    public static <T> T extractMethodAnnotation(Method method, Class<? extends Annotation> clazz) {
        if (method.isAnnotationPresent(clazz)) {
            Annotation annotation = method.getAnnotation(clazz);
            T routine = (T) annotation;
            return routine;
        }
        return null;
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
        return cfg != null && !Boolean.FALSE.equals(cfg.get(OpflowConstant.OPFLOW_COMMON_ENABLED));
    }
    
    public static boolean isAMQPEntrypointNull(Map<String, Object> cfg) {
        return isAMQPEntrypointNull(cfg,  OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_NAME, OpflowConstant.OPFLOW_PUBSUB_ROUTING_KEY);
    }
    
    public static boolean isAMQPEntrypointNull(Map<String, Object> cfg, String exchangeName, String routingKey) {
        return cfg.get(exchangeName) == null || cfg.get(routingKey) == null;
    }
    
    public static String getAMQPEntrypointCode(Map<String, Object> cfg) {
        return getAMQPEntrypointCode(cfg, OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_NAME, OpflowConstant.OPFLOW_PUBSUB_ROUTING_KEY);
    }
    
    public static String getAMQPEntrypointCode(Map<String, Object> cfg, String exchangeName, String routingKey) {
        String result = null;
        if (cfg.get(exchangeName) != null) {
            result = result + cfg.get(exchangeName).toString();
        }
        if (cfg.get(routingKey) != null) {
            result = result + cfg.get(routingKey);
        }
        return result;
    }
    
    public static String getClassSimpleName(Class clazz) {
        return clazz.getSimpleName();
    }
    
    public static String extractClassName(Class clazz) {
        return clazz.getName().replace(clazz.getPackage().getName(), "");
    }
    
    public static Throwable rebuildInvokerException(Map<String, Object> errorMap) {
        Object exceptionName = errorMap.get("exceptionClass");
        Object exceptionPayload = errorMap.get("exceptionPayload");
        if (exceptionName != null && exceptionPayload != null) {
            try {
                Class exceptionClass = Class.forName(exceptionName.toString());
                return (Throwable) OpflowJsonTool.toObject(exceptionPayload.toString(), exceptionClass);
            } catch (ClassNotFoundException ex) {
                return wrapUnknownException(errorMap);
            }
        }
        return wrapUnknownException(errorMap);
    }
    
    private static Throwable wrapUnknownException(Map<String, Object> errorMap) {
        if (errorMap.get("message") != null) {
            return new OpflowRequestFailureException(errorMap.get("message").toString());
        }
        return new OpflowRequestFailureException();
    }
    
    public static void sleep(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException ex) {}
    }
}
