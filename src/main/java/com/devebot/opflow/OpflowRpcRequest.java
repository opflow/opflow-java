package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowJsonTool;
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
public class OpflowRpcRequest implements Iterator, OpflowTimeout.Timeoutable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcRequest.class);
    private final OpflowLogTracer logRequest;
    private final String requestId;
    private final String requestTime;
    private final String routineId;
    private final long timeout;
    private final OpflowTimeout.Listener completeListener;
    private OpflowTimeout.Watcher timeoutWatcher;
    private long timestamp;
    
    public OpflowRpcRequest(Map<String, Object> options, final OpflowTimeout.Listener completeListener) {
        Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        this.requestId = OpflowUtil.getRequestId(opts);
        this.requestTime = OpflowUtil.getRequestTime(opts);
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

        logRequest = OpflowLogTracer.ROOT.branch("requestId", requestId, new OpflowLogTracer.OmitPingLogs(options))
                .branch("requestTime", requestTime);

        this.completeListener = completeListener;

        if (Boolean.TRUE.equals(opts.get("watcherEnabled")) && completeListener != null && this.timeout > 0) {
            timeoutWatcher = new OpflowTimeout.Watcher(requestId, this.timeout, new OpflowTimeout.Listener() {
                @Override
                public void handleEvent() {
                    OpflowLogTracer logWatcher = null;
                    if (logRequest.ready(LOG, "debug")) {
                        logWatcher = logRequest.copy();
                    }
                    if (logWatcher != null && logWatcher.ready(LOG, "debug")) LOG.debug(logWatcher
                            .text("Request[${requestId}] timeout event has been raised")
                            .stringify());
                    list.add(OpflowMessage.ERROR);
                    if (logWatcher != null && logWatcher.ready(LOG, "debug")) LOG.debug(logWatcher
                            .text("Request[${requestId}] raise completeListener (timeout)")
                            .stringify());
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

    public String getRequestTime() {
        return requestTime;
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
        return this.timestamp;
    }
    
    @Override
    public void raiseTimeout() {
        this.push(OpflowMessage.ERROR);
    }
    
    private final BlockingQueue<OpflowMessage> list = new LinkedBlockingQueue<>();
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
            OpflowLogTracer pushTrail = null;
            if (logRequest.ready(LOG, "debug")) {
                pushTrail = logRequest.copy();
            }
            if (pushTrail != null && pushTrail.ready(LOG, "debug")) LOG.debug(pushTrail
                    .text("Request[${requestId}] has completed/failed message")
                    .stringify());
            list.add(OpflowMessage.EMPTY);
            if (completeListener != null) {
                if (pushTrail != null && pushTrail.ready(LOG, "debug")) LOG.debug(pushTrail
                        .text("Request[${requestId}] raises completeListener (completed)")
                        .stringify());
                completeListener.handleEvent();
            }
            if (timeoutWatcher != null) {
                timeoutWatcher.close();
            }
        }
    }
    
    public List<OpflowMessage> iterateResult() {
        List<OpflowMessage> buff = new LinkedList<>();
        while(this.hasNext()) buff.add(this.next());
        return buff;
    }
    
    public OpflowRpcResult extractResult() {
        return extractResult(true);
    }
    
    public OpflowRpcResult extractResult(final boolean includeProgress) {
        OpflowLogTracer extractTrail = logRequest;
        if (extractTrail != null && extractTrail.ready(LOG, "trace")) LOG.trace(extractTrail
                .text("Request[${requestId}] - extracting result")
                .stringify());
        String consumerTag = null;
        boolean failed = false;
        byte[] error = null;
        boolean completed = false;
        byte[] value = null;
        List<OpflowRpcResult.Step> steps = new LinkedList<>();
        while(this.hasNext()) {
            OpflowMessage msg = this.next();
            String status = getStatus(msg);
            if (extractTrail != null && extractTrail.ready(LOG, "trace")) LOG.trace(extractTrail
                    .put("status", status)
                    .text("Request[${requestId}] - examine message, status: ${status}")
                    .stringify());
            if (status == null) continue;
            switch (status) {
                case "progress":
                    if (includeProgress) {
                        try {
                            int percent = OpflowJsonTool.extractFieldAsInt(msg.getBodyAsString(), "percent");
                            steps.add(new OpflowRpcResult.Step(percent));
                        } catch (OpflowJsonTransformationException jse) {
                            steps.add(new OpflowRpcResult.Step());
                        }
                    }   break;
                case "failed":
                    consumerTag = OpflowUtil.getMessageField(msg, "consumerTag");
                    failed = true;
                    error = msg.getBody();
                    break;
                case "completed":
                    consumerTag = OpflowUtil.getMessageField(msg, "consumerTag");
                    completed = true;
                    value = msg.getBody();
                    break;
                default:
                    break;
            }
        }
        if (extractTrail != null && extractTrail.ready(LOG, "trace")) LOG.trace(extractTrail
                .text("Request[${requestId}] - extracting result has completed")
                .stringify());
        if (!includeProgress) steps = null;
        return new OpflowRpcResult(routineId, requestId, consumerTag, steps, failed, error, completed, value);
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
