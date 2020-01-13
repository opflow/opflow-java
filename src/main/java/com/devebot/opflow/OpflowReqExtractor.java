package com.devebot.opflow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 *
 * @author pnhung177
 */
public class OpflowReqExtractor {

    private Class getRequestIdClass;
    private Method getRequestIdMethod;

    public OpflowReqExtractor() {
        this(null, null);
    }

    public OpflowReqExtractor(String getRequestIdClassName) {
        this(getRequestIdClassName, null);
    }

    public OpflowReqExtractor(String getRequestIdClassName, String getRequestIdMethodName) {
        init(getRequestIdClassName, getRequestIdMethodName);
    }
    
    public OpflowReqExtractor(Map<String, Object> kwargs) {
        if (kwargs == null) {
            init(null, null);
        } else {
            String className = null;
            if (kwargs.get("getRequestIdClassName") instanceof String) {
                className = (String) kwargs.get("getRequestIdClassName");
            }
            String methodName = null;
            if (kwargs.get("getRequestIdMethodName") instanceof String) {
                methodName = (String) kwargs.get("getRequestIdMethodName");
            }
            init(className, methodName);
        }
    }

    public String extractRequestId(Object[] args) {
        if (args == null) {
            return null;
        }
        for (Object arg : args) {
            if (getRequestIdClass.isInstance(arg)) {
                return invokeGetRequestId(arg);
            }
        }
        return OpflowUtil.getLogID();
    }

    private String invokeGetRequestId(Object arg) {
        try {
            return (String) getRequestIdMethod.invoke(arg);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            return null;
        }
    }

    private void init(String getRequestIdClassName, String getRequestIdMethodName) {
        String className = getRequestIdClassName != null ? getRequestIdClassName : OpflowReqIdentifiable.class.getName();
        String methodName = getRequestIdMethodName != null ? getRequestIdMethodName : "getRequestId";

        try {
            getRequestIdClass = Class.forName(className);
            getRequestIdMethod = getRequestIdClass.getDeclaredMethod(methodName);
        }
        catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
}
