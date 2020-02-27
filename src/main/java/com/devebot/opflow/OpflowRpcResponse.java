package com.devebot.opflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowOperationException;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResponse {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcResponse.class);
    private final OpflowLogTracer logTracer;
    private final String componentId;
    private final Channel channel;
    private final AMQP.BasicProperties properties;
    private final String consumerTag;
    private final String replyQueueName;
    private final String routineId;
    private final String routineTimestamp;
    private final String routineScope;
    private final Boolean progressEnabled;
    
    public OpflowRpcResponse(String componentId, Channel channel, AMQP.BasicProperties properties, String consumerTag, String replyQueueName, Map<String, Object> extras) {
        final Map<String, Object> headers = properties.getHeaders();
        
        this.componentId = componentId;
        this.channel = channel;
        this.properties = properties;
        this.consumerTag = consumerTag;
        
        this.routineId = OpflowUtil.getRoutineId(headers, false);
        this.routineTimestamp = OpflowUtil.getRoutineTimestamp(headers, false);
        
        logTracer = OpflowLogTracer.ROOT.branch(CONST.REQUEST_TIME, this.routineTimestamp)
                .branch(CONST.REQUEST_ID, this.routineId, new OpflowUtil.OmitInternalOplogs(headers));
        
        if (properties.getReplyTo() != null) {
            this.replyQueueName = properties.getReplyTo();
        } else {
            this.replyQueueName = replyQueueName;
        }
        
        this.routineScope = OpflowUtil.getOptionField(headers, CONST.AMQP_HEADER_ROUTINE_SCOPE, false);
        this.progressEnabled = (Boolean) OpflowUtil.getOptionField(headers, "progressEnabled", null);
        
        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .put("consumerTag", this.consumerTag)
                .put("replyTo", this.replyQueueName)
                .put("progressEnabled", this.progressEnabled)
                .text("Request[${requestId}][${requestTime}][x-rpc-response-created] - RpcResponse is created")
                .stringify());
    }
    
    public String getApplicationId() {
        return properties.getAppId();
    }

    public String getReplyQueueName() {
        return replyQueueName;
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public void emitStarted() {
        emitStarted("{}");
    }
    
    public void emitStarted(String content) {
        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .put("body", content)
                .text("Request[${requestId}][${requestTime}] - emitStarted()")
                .stringify());
        emitStarted(OpflowUtil.getBytes(content));
    }
    
    public void emitStarted(byte[] info) {
        if (info == null) info = new byte[0];
        basicPublish(info, createProperties(properties, createHeaders("started")).build());
        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .put("bodyLength", info.length)
                .text("Request[${requestId}][${requestTime}] - emitStarted()")
                .stringify());
    }
    
    public void emitProgress(int completed, int total) {
        emitProgress(completed, total, null);
    }
    
    public void emitProgress(int completed, int total, String jsonData) {
        if (progressEnabled != null && Boolean.FALSE.equals(progressEnabled)) return;
        int percent = -1;
        if (total > 0 && completed >= 0 && completed <= total) {
            percent = (total == 100) ? completed : Math.round((completed * 100) / total);
        }
        String result;
        if (jsonData == null) {
            result = "{ \"percent\": " + percent + " }";
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                    .put("body", result)
                    .put("bodyLength", result.length())
                    .text("Request[${requestId}][${requestTime}][x-rpc-response-emit-progress] - emitProgress()")
                    .stringify());
        } else {
            result = "{ \"percent\": " + percent + ", \"data\": " + jsonData + "}";
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                    .put("bodyLength", result.length())
                    .text("Request[${requestId}][${requestTime}][x-rpc-response-emit-progress] - emitProgress()")
                    .stringify());
        }
        basicPublish(OpflowUtil.getBytes(result), createProperties(properties, createHeaders("progress")).build());
    }
    
    public void emitFailed(String error) {
        emitFailed(OpflowUtil.getBytes(error));
    }
    
    public void emitFailed(byte[] error) {
        if (error == null) error = new byte[0];
        basicPublish(error, createProperties(properties, createHeaders("failed", true)).build());
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.trace(logTracer
                .put("bodyLength", error.length)
                .text("Request[${requestId}][${requestTime}][x-rpc-response-emit-failed] - emitFailed()")
                .stringify());
    }
    
    public void emitCompleted(String result) {
        emitCompleted(OpflowUtil.getBytes(result));
    }

    public void emitCompleted(byte[] result) {
        if (result == null) result = new byte[0];
        basicPublish(result, createProperties(properties, createHeaders("completed", true)).build());
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.trace(logTracer
                .put("bodyLength", result.length)
                .text("Request[${requestId}][${requestTime}][x-rpc-response-emit-completed] - emitCompleted()")
                .stringify());
    }

    private AMQP.BasicProperties.Builder createProperties(AMQP.BasicProperties properties, Map<String, Object> headers) {
        String expiration = properties.getExpiration();
        if (expiration == null) {
            expiration = "1000";
        }
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .headers(headers)
                .expiration(expiration)
                .correlationId(properties.getCorrelationId());
        if (properties.getAppId() != null) {
            builder.appId(properties.getAppId());
        }
        return builder;
    }
    
    private Map<String, Object> createHeaders(String status) {
        return createHeaders(status, false);
    }
    
    private Map<String, Object> createHeaders(String status, boolean finished) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("status", status);
        if (this.componentId != null) {
            headers.put(CONST.RPC_WORKER_ID, this.componentId);
        }
        if (this.routineId != null) {
            headers.put(CONST.AMQP_HEADER_ROUTINE_ID, this.routineId);
        }
        if (this.routineTimestamp != null) {
            headers.put(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, this.routineTimestamp);
        }
        if (this.routineScope != null) {
            headers.put(CONST.AMQP_HEADER_ROUTINE_SCOPE, this.routineScope);
        }
        if (finished) {
            headers.put(CONST.AMQP_HEADER_PROTOCOL_VERSION, CONST.AMQP_PROTOCOL_VERSION);
            headers.put("consumerTag", this.consumerTag);
        }
        return headers;
    }
    
    private void basicPublish(byte[] data, AMQP.BasicProperties replyProps) {
        try {
            channel.basicPublish("", replyQueueName, replyProps, data);
        } catch (IOException exception) {
            throw new OpflowOperationException(exception);
        }
    }
}
