package com.devebot.opflow;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Date;
import com.devebot.opflow.exception.OpflowOperationException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowUtil {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowUtil.class);
    
    private static final Gson gson = new Gson();
    private static final JsonParser jsonParser = new JsonParser();
    
    public static long getCurrentTime() {
        return (new Date()).getTime();
    }
    
    public static String getUUID() {
        return UUID.randomUUID().toString();
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
    
    public static String getStatus(OpflowMessage message) {
        if (message == null) return null;
        Map<String, Object> info = message.getInfo();
        if (info != null && info.get("status") != null) {
            return info.get("status").toString();
        }
        return null;
    }
    
    public static List<OpflowMessage> iterateRequest(OpflowRpcRequest request) {
        List<OpflowMessage> buff = new LinkedList<OpflowMessage>();
        while(request.hasNext()) buff.add(request.next());
        return buff;
    }
    
    public static OpflowRpcResult exhaustRequest(OpflowRpcRequest request) {
        return exhaustRequest(request, true);
    }
    
    public static OpflowRpcResult exhaustRequest(OpflowRpcRequest request, final boolean includeProgress) {
        String requestId = request.getRequestId();
        Iterator<OpflowMessage> iter = request;
        if (LOG.isTraceEnabled()) LOG.trace("Request[" + requestId + "] withdraw ...");
        byte[] error = null;
        byte[] value = null;
        List<OpflowRpcResult.Step> steps = new LinkedList<OpflowRpcResult.Step>();
        while(iter.hasNext()) {
            OpflowMessage msg = iter.next();
            String status = getStatus(msg);
            if (LOG.isTraceEnabled()) {
                LOG.trace(MessageFormat.format("Request[{0}] receive message with status: {1}", new Object[] {
                    requestId, status
                }));
            }
            if (status == null) continue;
            if ("progress".equals(status)) {
                if (includeProgress) {
                    try {
                        JsonObject jsonObject = (JsonObject)jsonParser.parse(msg.getContentAsString());
                        int percent = Integer.parseInt(jsonObject.get("percent").toString());
                        steps.add(new OpflowRpcResult.Step(percent));
                    } catch (JsonSyntaxException jse) {
                        steps.add(new OpflowRpcResult.Step());
                    } catch (NumberFormatException nfe) {
                        steps.add(new OpflowRpcResult.Step());
                    }
                }
            } else
            if ("failed".equals(status)) {
                error = msg.getContent();
            } else
            if ("completed".equals(status)) {
                value = msg.getContent();
            }
        }
        if (LOG.isTraceEnabled()) LOG.trace("Request[" + requestId + "] withdraw done");
        if (!includeProgress) steps = null;
        return new OpflowRpcResult(steps, error, value);
    }
}
