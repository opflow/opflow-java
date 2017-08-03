package com.devebot.opflow;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcRequest implements Iterator {

    private final Logger logger = LoggerFactory.getLogger(OpflowRpcRequest.class);
    private final String requestId;
    private final String routineId;
    private final Integer timeout;
    private final OpflowTask.Listener completeListener;
    private OpflowTask.TimeoutWatcher timeoutWatcher;
    private long timestamp;
    
    public OpflowRpcRequest(Map<String, Object> options, final OpflowTask.Listener completeListener) {
        Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        this.requestId = (opts.get("requestId") != null) ? (String)opts.get("requestId") : OpflowUtil.getUUID();
        this.routineId = (opts.get("routineId") != null) ? (String)opts.get("routineId") : OpflowUtil.getUUID();
        this.timeout = (Integer)opts.get("timeout");
        this.completeListener = completeListener;
        if (Boolean.TRUE.equals(opts.get("watcherEnabled")) && completeListener != null &&
                this.timeout != null && this.timeout > 0) {
            timeoutWatcher = new OpflowTask.TimeoutWatcher(this.timeout, new OpflowTask.Listener() {
                @Override
                public void handleEvent() {
                    if (logger.isDebugEnabled()) logger.debug("Request[" + requestId + "] timeout event has been raised");
                    list.add(OpflowMessage.ERROR);
                    if (logger.isDebugEnabled()) logger.debug("Request[" + requestId + "] raise completeListener (timeout)");
                    completeListener.handleEvent();
                }
            });
            timeoutWatcher.start();
        }
        checkTimestamp();
    }

    public String getRequestId() {
        return requestId;
    }
    
    public String getRoutineId() {
        return routineId;
    }

    public long getTimeout() {
        if (this.timeout == null || this.timeout <= 0) return 0;
        return 1000 * this.timeout;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    private final BlockingQueue<OpflowMessage> list = new LinkedBlockingQueue<OpflowMessage>();
    private OpflowMessage current = null;
    
    @Override
    public boolean hasNext() {
        try {
            this.current = list.take();
            if (this.current == OpflowMessage.EMPTY) return false;
            if (this.current == OpflowMessage.ERROR) return false;
            return true;
        } catch (InterruptedException ie) {
            return false;
        }
    }

    @Override
    public OpflowMessage next() {
        OpflowMessage result = this.current;
        this.current = null;
        return result;
    }
    
    public void push(OpflowMessage message) {
        list.add(message);
        if (timeoutWatcher != null) {
            timeoutWatcher.check();
        }
        checkTimestamp();
        if(isCompleted(message)) {
            if (logger.isDebugEnabled()) logger.debug("Request[" + requestId + "] completed/failed message");
            list.add(OpflowMessage.EMPTY);
            if (completeListener != null) {
                if (logger.isDebugEnabled()) logger.debug("Request[" + requestId + "] raise completeListener (completed)");
                completeListener.handleEvent();
            }
            if (timeoutWatcher != null) {
                timeoutWatcher.close();
            }
        }
    }
    
    public List<OpflowMessage> iterate() {
        List<OpflowMessage> buff = new LinkedList<OpflowMessage>();
        while(this.hasNext()) buff.add(this.next());
        return buff;
    }
    
    private final JsonParser jsonParser = new JsonParser();
    
    public OpflowRpcResult exhaust() {
        return exhaust(true);
    }
    
    public OpflowRpcResult exhaust(final boolean includeProgress) {
        if (logger.isTraceEnabled()) logger.trace("Request[" + requestId + "] withdraw ...");
        byte[] error = null;
        byte[] value = null;
        List<OpflowRpcResult.Step> steps = new LinkedList<OpflowRpcResult.Step>();
        while(this.hasNext()) {
            OpflowMessage msg = this.next();
            String status = getStatus(msg);
            if (logger.isTraceEnabled()) {
                logger.trace(MessageFormat.format("Request[{0}] receive message with status: {1}", new Object[] {
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
        if (logger.isTraceEnabled()) logger.trace("Request[" + requestId + "] withdraw done");
        if (!includeProgress) steps = null;
        return new OpflowRpcResult(steps, error, value);
    }
    
    public void exit() {
        exit(true);
    }
    
    public void exit(boolean error) {
        list.add(error ? OpflowMessage.ERROR : OpflowMessage.EMPTY);
    }
    
    private static final List<String> STATUS = Arrays.asList(new String[] { "failed", "completed" });
    
    private String getStatus(OpflowMessage message) {
        if (message == null) return null;
        Map<String, Object> info = message.getInfo();
        if (info != null && info.get("status") != null) {
            return info.get("status").toString();
        }
        return null;
    }
    
    private boolean isCompleted(OpflowMessage message) {
        String status = getStatus(message);
        if (status == null) return false;
        return STATUS.indexOf(status) >= 0;
    }
    
    private void checkTimestamp() {
        timestamp = OpflowUtil.getCurrentTime();
    }
}
