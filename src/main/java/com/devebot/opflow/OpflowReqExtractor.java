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
            uuidIfNotFound = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_REQ_EXTRACTOR_AUTO_UUID, true);
            String className = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_REQ_EXTRACTOR_CLASS_NAME);
            String methodName = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_REQ_EXTRACTOR_METHOD_NAME);
            init(className, methodName);
        }
    }

    public String getGetRequestIdSignature() {
        if (getRequestIdMethod == null) return null;
        return getRequestIdMethod.toString();
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
            return OpflowUUID.getBase64ID();
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
