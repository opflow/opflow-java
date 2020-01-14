package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowReqIdentifiableException;
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
    private boolean uuidIfNotFound = true;

    public OpflowReqExtractor() throws OpflowReqIdentifiableException {
        this(null, null);
    }

    public OpflowReqExtractor(String getRequestIdClassName) throws OpflowReqIdentifiableException {
        this(getRequestIdClassName, null);
    }

    public OpflowReqExtractor(String getRequestIdClassName, String getRequestIdMethodName) throws OpflowReqIdentifiableException {
        init(getRequestIdClassName, getRequestIdMethodName);
    }

    public OpflowReqExtractor(Map<String, Object> kwargs) throws OpflowReqIdentifiableException {
        if (kwargs == null) {
            init(null, null);
        } else {
            if (kwargs.get("uuidIfNotFound") instanceof Boolean) {
                uuidIfNotFound = (Boolean) kwargs.get("uuidIfNotFound");
            }
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
        if (uuidIfNotFound) {
            return OpflowUUID.getLogID();
        }
        return null;
    }

    private String invokeGetRequestId(Object arg) {
        try {
            return (String) getRequestIdMethod.invoke(arg);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            return null;
        }
    }

    private void init(String getRequestIdClassName, String getRequestIdMethodName) throws OpflowReqIdentifiableException {
        String className = getRequestIdClassName != null ? getRequestIdClassName : OpflowReqIdentifiable.class.getName();
        String methodName = getRequestIdMethodName != null ? getRequestIdMethodName : "getRequestId";

        try {
            getRequestIdClass = Class.forName(className);
            getRequestIdMethod = getRequestIdClass.getDeclaredMethod(methodName);
        }
        catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            throw new OpflowReqIdentifiableException(e);
        }

        Class returnType = getRequestIdMethod.getReturnType();
        if (!returnType.equals(String.class)) {
            throw new OpflowReqIdentifiableException("Method [" + methodName + "] return type must be String");
        }
    }
}
