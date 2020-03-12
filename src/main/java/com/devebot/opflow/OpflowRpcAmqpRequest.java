package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.exception.OpflowJsonTransformationException;
import java.util.Arrays;
import java.util.Date;
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
public class OpflowRpcAmqpRequest implements Iterator, OpflowTimeout.Timeoutable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcAmqpRequest.class);
    private final OpflowLogTracer reqTracer;
    private final String routineId;
    private final String routineTimestamp;
    private final String routineSignature;
    private final long timeout;
    private final OpflowTimeout.Listener completeListener;
    private long timestamp;

    public OpflowRpcAmqpRequest(final OpflowRpcParameter params, final OpflowTimeout.Listener completeListener) {
        this.routineId = params.getRoutineId();
        this.routineSignature = params.getRoutineSignature();
        this.routineTimestamp = params.getRoutineTimestamp();
        
        if (params.getRoutineTTL() == null) {
            this.timeout = 0l;
        } else {
            this.timeout = params.getRoutineTTL();
        }
        
        reqTracer = OpflowLogTracer.ROOT.branch(CONST.REQUEST_TIME, this.routineTimestamp)
                .branch(CONST.REQUEST_ID, this.routineId, params);
        
        this.completeListener = completeListener;
        
        checkTimestamp();
    }
    
    public OpflowRpcAmqpRequest(final Map<String, Object> options, final OpflowTimeout.Listener completeListener) {
        this(new OpflowRpcParameter(options), completeListener);
    }
    
    public String getRoutineId() {
        return routineId;
    }

    public String getRoutineTimestamp() {
        return routineTimestamp;
    }

    public String getRoutineSignature() {
        return routineSignature;
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
        checkTimestamp();
        if(isDone(message)) {
            OpflowLogTracer pushTrail = null;
            if (reqTracer.ready(LOG, Level.DEBUG)) {
                pushTrail = reqTracer.copy();
            }
            if (pushTrail != null && pushTrail.ready(LOG, Level.DEBUG)) LOG.debug(pushTrail
                    .text("Request[${requestId}][${requestTime}][x-rpc-request-finished] has completed/failed message")
                    .stringify());
            list.add(OpflowMessage.EMPTY);
            if (completeListener != null) {
                if (pushTrail != null && pushTrail.ready(LOG, Level.DEBUG)) LOG.debug(pushTrail
                        .text("Request[${requestId}][${requestTime}][x-rpc-request-callback] raises completeListener (completed)")
                        .stringify());
                completeListener.handleEvent();
            }
        }
    }
    
    public List<OpflowMessage> iterateResult() {
        List<OpflowMessage> buff = new LinkedList<>();
        while(this.hasNext()) buff.add(this.next());
        return buff;
    }
    
    public OpflowRpcAmqpResult extractResult() {
        return extractResult(true);
    }
    
    public OpflowRpcAmqpResult extractResult(final boolean includeProgress) {
        OpflowLogTracer extractTrail = reqTracer;
        if (extractTrail != null && extractTrail.ready(LOG, Level.TRACE)) LOG.trace(extractTrail
                .text("Request[${requestId}][${requestTime}][x-rpc-request-extract-result-begin] - extracting result")
                .stringify());
        String consumerTag = null;
        boolean failed = false;
        byte[] error = null;
        boolean completed = false;
        byte[] value = null;
        List<OpflowRpcAmqpResult.Step> steps = new LinkedList<>();
        while(this.hasNext()) {
            OpflowMessage msg = this.next();
            String status = getStatus(msg);
            if (extractTrail != null && extractTrail.ready(LOG, Level.TRACE)) LOG.trace(extractTrail
                    .put("status", status)
                    .text("Request[${requestId}][${requestTime}][x-rpc-request-examine-status] - examine message, status: ${status}")
                    .stringify());
            if (status == null) continue;
            switch (status) {
                case "progress":
                    if (includeProgress) {
                        try {
                            int percent = OpflowJsonTool.extractFieldAsInt(msg.getBodyAsString(), "percent");
                            steps.add(new OpflowRpcAmqpResult.Step(percent));
                        } catch (OpflowJsonTransformationException jse) {
                            steps.add(new OpflowRpcAmqpResult.Step());
                        }
                    }   break;
                case "failed":
                    consumerTag = getMessageField(msg, CONST.AMQP_HEADER_CONSUMER_TAG);
                    failed = true;
                    error = msg.getBody();
                    break;
                case "completed":
                    consumerTag = getMessageField(msg, CONST.AMQP_HEADER_CONSUMER_TAG);
                    completed = true;
                    value = msg.getBody();
                    break;
                default:
                    break;
            }
        }
        if (extractTrail != null && extractTrail.ready(LOG, Level.TRACE)) LOG.trace(extractTrail
                .text("Request[${requestId}][${requestTime}][x-rpc-request-extract-result-end] - extracting result has completed")
                .stringify());
        if (!includeProgress) steps = null;
        return new OpflowRpcAmqpResult(routineSignature, routineId, consumerTag, steps, failed, error, completed, value);
    }
    
    private static final List<String> STATUS = Arrays.asList(new String[] { "failed", "completed" });
    
    private boolean isDone(OpflowMessage message) {
        String status = getStatus(message);
        if (status == null) return false;
        return STATUS.indexOf(status) >= 0;
    }
    
    private void checkTimestamp() {
        timestamp = (new Date()).getTime();
    }
    
    private static String getStatus(OpflowMessage message) {
        return getMessageField(message, CONST.AMQP_HEADER_RETURN_STATUS);
    }
    
    private static String getMessageField(OpflowMessage message, String fieldName) {
        if (message == null || fieldName == null) return null;
        Map<String, Object> info = message.getHeaders();
        if (info != null) {
            Object val = info.get(fieldName);
            if (val != null) {
                return val.toString();
            }
        }
        return null;
    }
}
