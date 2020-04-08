package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.supports.OpflowConverter;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowEnvTool;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowNetTool;
import com.devebot.opflow.supports.OpflowStringUtil;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

/**
 *
 * @author drupalex
 */
public class OpflowUtil {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    
    private final static boolean IS_PING_LOGGING_OMITTED;
        private final static boolean IS_EXIT_ON_ERROR;
    
    static {
        IS_PING_LOGGING_OMITTED = !"false".equals(ENVTOOL.getEnvironVariable("OPFLOW_OMIT_PING_LOGS", null));
        IS_EXIT_ON_ERROR = !"false".equals(ENVTOOL.getEnvironVariable("OPFLOW_EXIT_ON_ERROR", null));
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
            this(getStringField(options, CONST.AMQP_HEADER_ROUTINE_SCOPE, false, false));
        }
        
        @Override
        public boolean isMute() {
            return IS_PING_LOGGING_OMITTED && isInternalOplog;
        }
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
        copyParameters(target, source, keys, true);
    }
    
    public static void copyParameters(Map<String, Object> target, Map<String, Object> source, String[] keys, boolean overridden) {
        if (source == null || keys == null) {
            return;
        }
        for(String field: keys) {
            if (source.containsKey(field)) {
                if (overridden || !target.containsKey(field)) {
                    target.put(field, source.get(field));
                }
            }
        }
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
    
    public static String getStringField(Map<String, Object> options, String fieldName) {
        return getStringField(options, fieldName, null, null, false, false);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, String[] otherNames) {
        return getStringField(options, fieldName, otherNames, null, false, false);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, String defValue) {
        return getStringField(options, fieldName, null, defValue, false, false);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, boolean uuidIfNotFound) {
        return getStringField(options, fieldName, null, null, uuidIfNotFound, false);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, boolean uuidIfNotFound, boolean assigned) {
        return getStringField(options, fieldName, null, null, uuidIfNotFound, assigned);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, String defValue, boolean uuidIfNotFound, boolean assigned) {
        return getStringField(options, fieldName, null, defValue, uuidIfNotFound, assigned);
    }
    
    public static String getStringField(Map<String, Object> options, String fieldName, String[] otherNames, String defValue, boolean uuidIfNotFound, boolean assigned) {
        if (options == null || fieldName == null) {
            return null;
        }
        Object value = options.get(fieldName);
        if (value != null) {
            return value.toString();
        }
        if (otherNames != null) {
            for (String otherName : otherNames) {
                value = options.get(otherName);
                if (value != null) {
                    String valueStr = value.toString();
                    if (assigned) {
                        options.put(fieldName, valueStr);
                    }
                    return valueStr;
                }
            }
        }
        String valueStr = (defValue != null) ? defValue : (uuidIfNotFound ? OpflowUUID.getBase64ID() : null);
        if (assigned) {
            options.put(fieldName, valueStr);
        }
        return valueStr;
    }
    
    public static void setStringField(Map<String, Object> options, String fieldName, String value) {
        if (options == null) return;
        options.put(fieldName, value);
    }
    
    public static String[] getStringArray(Map<String, Object> options, String fieldName, String[] defArray) {
        Object value = options.get(fieldName);
        if (value instanceof String[]) {
            return (String[]) value;
        }
        return defArray;
    }
    
    public static Boolean getBooleanField(Map<String, Object> options, String fieldName, Boolean defValue) {
        return getPrimitiveField(options, fieldName, null, defValue, Boolean.class, false);
    }
    
    public static Boolean getBooleanField(Map<String, Object> options, String fieldName, String[] otherNames, Boolean defValue, boolean assigned) {
        return getPrimitiveField(options, fieldName, otherNames, defValue, Boolean.class, assigned);
    }
    
    public static Integer getIntegerField(Map<String, Object> options, String fieldName, Integer defValue) {
        return getPrimitiveField(options, fieldName, null, defValue, Integer.class, false);
    }
    
    public static Integer getIntegerField(Map<String, Object> options, String fieldName, String[] otherNames, Integer defValue, boolean assigned) {
        return getPrimitiveField(options, fieldName, otherNames, defValue, Integer.class, assigned);
    }
    
    public static Long getLongField(Map<String, Object> options, String fieldName, Long defValue) {
        return getPrimitiveField(options, fieldName, null, defValue, Long.class, false);
    }
    
    public static Long getLongField(Map<String, Object> options, String fieldName, String[] otherNames, Long defValue, boolean assigned) {
        return getPrimitiveField(options, fieldName, otherNames, defValue, Long.class, assigned);
    }
    
    private static <T> T getPrimitiveField(Map<String, Object> options, String fieldName, String[] otherNames, T defValue, Class<T> type, boolean assigned) {
        if (options == null || fieldName == null) {
            return null;
        }
        T value = assertPrimitiveValue(options, fieldName, fieldName, type, assigned);
        if (value != null) {
            return value;
        }
        if (otherNames != null) {
            for (String otherName : otherNames) {
                value = assertPrimitiveValue(options, otherName, fieldName, type, assigned);
                if (value != null) {
                    return value;
                }
            }
        }
        if (assigned) {
            if (defValue != null) {
                options.put(fieldName, defValue);
            }
        }
        return defValue;
    }
    
    private static <T> T assertPrimitiveValue(Map<String, Object> options, String sourceName, String targetName, Class<T> type, boolean assigned) {
        Object value = options.get(sourceName);
        if (type.isInstance(value)) {
            return (T) value;
        }
        if (value != null) {
            T output = OpflowConverter.convert(value.toString(), type);
            if (assigned) {
                options.put(targetName, output);
            }
            return output;
        }
        return null;
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
    
    public static boolean isComponentExplicitEnabled(Map<String, Object> cfg) {
        return cfg != null && Boolean.TRUE.equals(cfg.get(OpflowConstant.OPFLOW_COMMON_ENABLED));
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
    
    public static boolean exitOnError() {
        return IS_EXIT_ON_ERROR;
    }
    
    public static void exit(Throwable t) {
        System.err.println("[!] There is a serious error and the service cannot be started.");
        if (t != null) {
            t.printStackTrace();
        }
        Runtime.getRuntime().exit(-1);
    }
}
