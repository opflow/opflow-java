package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowJsonTransformationException;
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
public class OpflowRpcRequest implements Iterator, OpflowTask.Timeoutable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcRequest.class);
    private final OpflowLogTracer logTracer;
    private final String requestId;
    private final String routineId;
    private final long timeout;
    private final OpflowTask.Listener completeListener;
    private OpflowTask.TimeoutWatcher timeoutWatcher;
    private long timestamp;
    
    public OpflowRpcRequest(Map<String, Object> options, final OpflowTask.Listener completeListener) {
        Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        this.requestId = OpflowUtil.getRequestId(opts);
        this.routineId = OpflowUtil.getRoutineId(opts);
        if (opts.get("timeout") == null) {
            this.timeout = 0;
        } else if (opts.get("timeout") instanceof Long) {
            this.timeout = (Long)opts.get("timeout");
        } else if (opts.get("timeout") instanceof Integer) {
            this.timeout = ((Integer)opts.get("timeout")).longValue();
        } else {
            this.timeout = 0;
        }
        logTracer = OpflowLogTracer.ROOT.branch("requestId", requestId);
        this.completeListener = completeListener;
        if (Boolean.TRUE.equals(opts.get("watcherEnabled")) && completeListener != null && this.timeout > 0) {
            timeoutWatcher = new OpflowTask.TimeoutWatcher(requestId, this.timeout, new OpflowTask.Listener() {
                @Override
                public void handleEvent() {
                    OpflowLogTracer logWatcher = logTracer.copy();
                    if (LOG.isDebugEnabled()) LOG.debug(logWatcher
                            .put("message", "Request timeout event has been raised")
                            .toString());
                    list.add(OpflowMessage.ERROR);
                    if (LOG.isDebugEnabled()) LOG.debug(logWatcher
                            .put("message", "Request raise completeListener (timeout)")
                            .toString());
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

    @Override
    public long getTimeout() {
        if (this.timeout <= 0) return 0;
        return this.timeout;
    }
    
    @Override
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public void raiseTimeout() {
        list.add(OpflowMessage.ERROR);
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
        if(isDone(message)) {
            if (LOG.isDebugEnabled()) LOG.debug(logTracer
                    .put("message", "Request has completed/failed message")
                    .toString());
            list.add(OpflowMessage.EMPTY);
            if (completeListener != null) {
                if (LOG.isDebugEnabled()) LOG.debug(logTracer
                        .put("message", "Request raises completeListener (completed)")
                        .toString());
                completeListener.handleEvent();
            }
            if (timeoutWatcher != null) {
                timeoutWatcher.close();
            }
        }
    }
    
    public List<OpflowMessage> iterateResult() {
        List<OpflowMessage> buff = new LinkedList<OpflowMessage>();
        while(this.hasNext()) buff.add(this.next());
        return buff;
    }
    
    public OpflowRpcResult extractResult() {
        return extractResult(true);
    }
    
    public OpflowRpcResult extractResult(final boolean includeProgress) {
        OpflowRpcRequest request = this;
        Iterator<OpflowMessage> iter = request;
        if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                .put("message", "Extracting result is running")
                .toString());
        String workerTag = null;
        boolean failed = false;
        byte[] error = null;
        boolean completed = false;
        byte[] value = null;
        List<OpflowRpcResult.Step> steps = new LinkedList<OpflowRpcResult.Step>();
        while(iter.hasNext()) {
            OpflowMessage msg = iter.next();
            String status = getStatus(msg);
            if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                    .put("status", status)
                    .put("message", "Extracting result receives a message")
                    .toString());
            if (status == null) continue;
            if ("progress".equals(status)) {
                if (includeProgress) {
                    try {
                        int percent = OpflowUtil.jsonExtractFieldAsInt(msg.getBodyAsString(), "percent");
                        steps.add(new OpflowRpcResult.Step(percent));
                    } catch (OpflowJsonTransformationException jse) {
                        steps.add(new OpflowRpcResult.Step());
                    }
                }
            } else
            if ("failed".equals(status)) {
                workerTag = OpflowUtil.getMessageField(msg, "workerTag");
                failed = true;
                error = msg.getBody();
            } else
            if ("completed".equals(status)) {
                workerTag = OpflowUtil.getMessageField(msg, "workerTag");
                completed = true;
                value = msg.getBody();
            }
        }
        if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                .put("message", "Extracting result has completed")
                .toString());
        if (!includeProgress) steps = null;
        return new OpflowRpcResult(routineId, requestId, workerTag, steps, failed, error, completed, value);
    }
    
    private static final List<String> STATUS = Arrays.asList(new String[] { "failed", "completed" });
    
    private boolean isDone(OpflowMessage message) {
        String status = getStatus(message);
        if (status == null) return false;
        return STATUS.indexOf(status) >= 0;
    }
    
    private void checkTimestamp() {
        timestamp = OpflowUtil.getCurrentTime();
    }
    
    public static String getStatus(OpflowMessage message) {
        return OpflowUtil.getMessageField(message, "status");
    }
}
