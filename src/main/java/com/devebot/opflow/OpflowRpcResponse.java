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

    final Logger logger = LoggerFactory.getLogger(OpflowRpcResponse.class);
    
    private final Channel channel;
    private final AMQP.BasicProperties properties;
    private final String workerTag;
    private final String replyQueueName;
    private String requestId;
    
    public OpflowRpcResponse(Channel channel, AMQP.BasicProperties properties, String workerTag, String replyQueueName) {
        this.channel = channel;
        this.properties = properties;
        this.workerTag = workerTag;
        
        if (properties.getReplyTo() != null) {
            this.replyQueueName = properties.getReplyTo();
        } else {
            this.replyQueueName = replyQueueName;
        }
        
        Map<String, Object> headers = OpflowUtil.getHeaders(properties);
        if (headers.get("requestId") != null) {
            this.requestId = headers.get("requestId").toString();
            if (logger.isTraceEnabled()) logger.trace("requestId: " + this.requestId);
        } else {
            if (logger.isTraceEnabled()) logger.trace("requestId is empty");
        }
        
        if (logger.isTraceEnabled()) {
            logger.trace("Request[" + this.requestId + "] will reply to: " + this.replyQueueName);
        }
    }
    
    public void emitStarted() {
        emitStarted("{}");
    }
    
    public void emitStarted(String content) {
        if (logger.isTraceEnabled()) {
            logger.trace("Request[" + this.requestId + "] emitStarted() - parameter: " + content);
        }
        emitStarted(OpflowUtil.getBytes(content));
    }
    
    public void emitStarted(byte[] info) {
        if (info == null) info = new byte[0];
        basicPublish(info, createProperties(properties, createHeaders("started")).build());
        if (logger.isTraceEnabled()) {
            logger.trace("Request[" + this.requestId + "] emitStarted() - byte[].length: " + info.length);
        }
    }
    
    public void emitProgress(int completed, int total) {
        emitProgress(completed, total, null);
    }
    
    public void emitProgress(int completed, int total, String jsonData) {
        int percent = -1;
        if (total > 0 && completed >= 0 && completed <= total) {
            percent = (total == 100) ? completed : Math.round((completed * 100) / total);
        }
        String result;
        if (jsonData == null) {
            result = "{ \"percent\": " + percent + " }";
        } else {
            result = "{ \"percent\": " + percent + ", \"data\": " + jsonData + "}";
        }
        basicPublish(OpflowUtil.getBytes(result), createProperties(properties, createHeaders("progress")).build());
        if (logger.isTraceEnabled()) {
            logger.trace("Request[" + this.requestId + "] emitProgress(): " + result);
        }
    }
    
    public void emitFailed(String error) {
        emitFailed(OpflowUtil.getBytes(error));
    }
    
    public void emitFailed(byte[] error) {
        if (error == null) error = new byte[0];
        basicPublish(error, createProperties(properties, createHeaders("failed", true)).build());
        if (logger.isTraceEnabled()) {
            logger.trace("Request[" + this.requestId + "] emitFailed() - byte[].length: " + error.length);
        }
    }
    
    public void emitCompleted(String result) {
        emitCompleted(OpflowUtil.getBytes(result));
    }

    public void emitCompleted(byte[] result) {
        if (result == null) result = new byte[0];
        basicPublish(result, createProperties(properties, createHeaders("completed", true)).build());
        if (logger.isTraceEnabled()) {
            logger.trace("Request[" + this.requestId + "] emitCompleted() - byte[].length: " + result.length);
        }
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
        Map<String, Object> headers = new HashMap<String, Object>();
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
