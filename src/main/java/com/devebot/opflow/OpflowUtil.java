package com.devebot.opflow;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.devebot.opflow.exception.OpflowOperationException;
import java.util.ArrayList;

/**
 *
 * @author drupalex
 */
public class OpflowUtil {
    private static final Gson gson = new Gson();
    
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
    
    public static Map<String, Object> cloneParameters(Map<String, Object> params) {
        Map<String, Object> clonedParams = new HashMap<String, Object>();
        for (String i : params.keySet()) {
            Object value = params.get(i);
            clonedParams.put(i, value);
        }
        return clonedParams;
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
    
    public static String buildJson(MapListener listener) {
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        if (listener != null) {
            listener.transform(jsonMap);
        }
        return gson.toJson(jsonMap);
    }
    
    public static Map<String, Object> buildOptions(MapListener listener) {
        return buildOptions(listener, null);
    }
    
    public static Map<String, Object> buildOptions(MapListener listener, Map<String, Object> defaultOpts) {
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        if (listener != null) {
            listener.transform(jsonMap);
        }
        return jsonMap;
    }
    
    public static Map<String, Object> ensureNotNull(Map<String, Object> opts) {
        return (opts == null) ? new HashMap<String, Object>() : opts;
    }
    
    public static Map<String, Object> getHeaders(AMQP.BasicProperties properties) {
        if (properties != null && properties.getHeaders() != null) {
            return properties.getHeaders();
        } else {
            return new HashMap<String, Object>();
        }
    }

    public static String getRequestID(Map<String, Object> headers) {
        if (headers == null) return UUID.randomUUID().toString();
        Object requestID = headers.get("requestId");
        if (requestID == null) return UUID.randomUUID().toString();
        return requestID.toString();
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
}
