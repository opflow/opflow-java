package com.devebot.opflow;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 *
 * @author pnhung177
 */
public class OpflowRequestIdExtractor {

    private Class getRequestIdClass;
    private Method getRequestIdMethod;

    public OpflowRequestIdExtractor() {
        this(null, null);
    }

    public OpflowRequestIdExtractor(String getRequestIdClassName) {
        this(getRequestIdClassName, null);
    }

    public OpflowRequestIdExtractor(String getRequestIdClassName, String getRequestIdMethodName) {
        String className = getRequestIdClassName != null ? getRequestIdClassName : OpflowRequestIdGettable.class.getName();
        String methodName = getRequestIdMethodName != null ? getRequestIdMethodName : "getRequestId";

        try {
            getRequestIdClass = Class.forName(className);
            getRequestIdMethod = getRequestIdClass.getDeclaredMethod(methodName);
        }
        catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
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
        return null;
    }

    private String invokeGetRequestId(Object arg) {
        try {
            return (String) getRequestIdMethod.invoke(arg);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            return null;
        }
    }
}
