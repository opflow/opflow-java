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
    private final String workerTag;
    private final String replyQueueName;

    public String getRequestId() {
        return requestId;
    }

    public String getReplyQueueName() {
        return replyQueueName;
    }

    public String getWorkerTag() {
        return workerTag;
    }

    private final String requestId;
    private final Boolean progressEnabled;
    
    public OpflowRpcResponse(Channel channel, AMQP.BasicProperties properties, String workerTag, String replyQueueName) {
        this.channel = channel;
        this.properties = properties;
        this.workerTag = workerTag;
        this.requestId = OpflowUtil.getRequestId(properties.getHeaders(), false);
        
        logTracer = OpflowLogTracer.ROOT.branch("requestId", this.requestId);
        
        if (properties.getReplyTo() != null) {
            this.replyQueueName = properties.getReplyTo();
        } else {
            this.replyQueueName = replyQueueName;
        }
        
        this.progressEnabled = (Boolean) OpflowUtil.getOptionField(properties.getHeaders(), "progressEnabled", null);
        
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .put("workerTag", this.workerTag)
                .put("replyTo", this.replyQueueName)
                .put("progressEnabled", this.progressEnabled)
                .text("RpcResponse is created")
                .stringify());
    }
    
    
    
    public void emitStarted() {
        emitStarted("{}");
    }
    
    public void emitStarted(String content) {
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .put("body", content)
                .text("emitStarted()")
                .stringify());
        emitStarted(OpflowUtil.getBytes(content));
    }
    
    public void emitStarted(byte[] info) {
        if (info == null) info = new byte[0];
        basicPublish(info, createProperties(properties, createHeaders("started")).build());
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .put("bodyLength", info.length)
                .text("emitStarted()")
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
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                    .put("body", result)
                    .put("bodyLength", result.length())
                    .text("emitProgress()")
                    .stringify());
        } else {
            result = "{ \"percent\": " + percent + ", \"data\": " + jsonData + "}";
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                    .put("bodyLength", result.length())
                    .text("emitProgress()")
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
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .put("bodyLength", error.length)
                .text("emitFailed()")
                .stringify());
    }
    
    public void emitCompleted(String result) {
        emitCompleted(OpflowUtil.getBytes(result));
    }

    public void emitCompleted(byte[] result) {
        if (result == null) result = new byte[0];
        basicPublish(result, createProperties(properties, createHeaders("completed", true)).build());
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .put("bodyLength", result.length)
                .text("emitCompleted()")
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
        if (finished) {
            headers.put("workerTag", this.workerTag);
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
