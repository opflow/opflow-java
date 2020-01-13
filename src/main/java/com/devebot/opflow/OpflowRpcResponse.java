package com.devebot.opflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.exception.OpflowOperationException;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResponse {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcResponse.class);
    private final OpflowLogTracer logTracer;
    private final Channel channel;
    private final AMQP.BasicProperties properties;
    private final String consumerTag;
    private final String replyQueueName;
    private final String requestId;
    private final String requestTime;
    private final String messageScope;
    private final Boolean progressEnabled;
    
    public OpflowRpcResponse(Channel channel, AMQP.BasicProperties properties, String consumerTag, String replyQueueName) {
        final Map<String, Object> headers = properties.getHeaders();
        
        this.channel = channel;
        this.properties = properties;
        this.consumerTag = consumerTag;
        
        this.requestId = OpflowUtil.getRequestId(headers, false);
        this.requestTime = OpflowUtil.getRequestTime(headers, false);
        
        logTracer = OpflowLogTracer.ROOT.branch("requestTime", requestTime)
                .branch("requestId", this.requestId, new OpflowLogTracer.OmitPingLogs(headers));
        
        if (properties.getReplyTo() != null) {
            this.replyQueueName = properties.getReplyTo();
        } else {
            this.replyQueueName = replyQueueName;
        }
        
        this.messageScope = OpflowUtil.getOptionField(headers, "messageScope", false);
        this.progressEnabled = (Boolean) OpflowUtil.getOptionField(headers, "progressEnabled", null);
        
        if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .put("consumerTag", this.consumerTag)
                .put("replyTo", this.replyQueueName)
                .put("progressEnabled", this.progressEnabled)
                .text("Request[${requestId}] - RpcResponse is created")
                .stringify());
    }
    
    public String getApplicationId() {
        return properties.getAppId();
    }
    
    public String getRequestId() {
        return requestId;
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
        if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .put("body", content)
                .text("Request[${requestId}] - emitStarted()")
                .stringify());
        emitStarted(OpflowUtil.getBytes(content));
    }
    
    public void emitStarted(byte[] info) {
        if (info == null) info = new byte[0];
        basicPublish(info, createProperties(properties, createHeaders("started")).build());
        if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .put("bodyLength", info.length)
                .text("Request[${requestId}] - emitStarted()")
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
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                    .put("body", result)
                    .put("bodyLength", result.length())
                    .text("Request[${requestId}] - emitProgress()")
                    .stringify());
        } else {
            result = "{ \"percent\": " + percent + ", \"data\": " + jsonData + "}";
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                    .put("bodyLength", result.length())
                    .text("Request[${requestId}] - emitProgress()")
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
        if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .put("bodyLength", error.length)
                .text("Request[${requestId}] - emitFailed()")
                .stringify());
    }
    
    public void emitCompleted(String result) {
        emitCompleted(OpflowUtil.getBytes(result));
    }

    public void emitCompleted(byte[] result) {
        if (result == null) result = new byte[0];
        basicPublish(result, createProperties(properties, createHeaders("completed", true)).build());
        if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .put("bodyLength", result.length)
                .text("Request[${requestId}] - emitCompleted()")
                .stringify());
    }

    private AMQP.BasicProperties.Builder createProperties(AMQP.BasicProperties properties, Map<String, Object> headers) {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
            .headers(headers)
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
        if (this.requestId != null) {
            headers.put("requestId", this.requestId);
        }
        if (this.requestTime != null) {
            headers.put("requestTime", this.requestTime);
        }
        if (this.messageScope != null) {
            headers.put("messageScope", this.messageScope);
        }
        if (finished) {
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
