package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubHandler implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPubsubHandler.class);
    private final OpflowLogTracer logTracer;

    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    private final String subscriberName;
    private final String recyclebinName;
    private final List<OpflowEngine.ConsumerInfo> consumerInfos = new LinkedList<>();
    private int prefetch = 0;
    private int subscriberLimit = 0;
    private int redeliveredLimit = 0;
    private OpflowPubsubListener listener;

    public OpflowPubsubHandler(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowUtil.ensureNotNull(params);
        
        final String pubsubHandlerId = OpflowUtil.getOptionField(params, "pubsubHandlerId", true);
        logTracer = OpflowLogTracer.ROOT.branch("pubsubHandlerId", pubsubHandlerId);
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("PubsubHandler[${pubsubHandlerId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put("engineId", pubsubHandlerId);
        brokerParams.put("mode", "pubsub");
        brokerParams.put("exchangeType", "direct");
        
        subscriberName = (String) params.get("subscriberName");
        recyclebinName = (String) params.get("recyclebinName");
        
        if (subscriberName != null && recyclebinName != null && subscriberName.equals(recyclebinName)) {
            throw new OpflowBootstrapException("subscriberName should be different with recyclebinName");
        }
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        if (subscriberName != null) {
            executor.assertQueue(subscriberName);
        }
        
        if (recyclebinName != null) {
            executor.assertQueue(recyclebinName);
        }
        
        if (params.get("prefetch") instanceof Integer) {
            prefetch = (Integer) params.get("prefetch");
            if (prefetch < 0) prefetch = 0;
        }
        
        if (params.get("subscriberLimit") instanceof Integer) {
            subscriberLimit = (Integer) params.get("subscriberLimit");
            if (subscriberLimit < 0) subscriberLimit = 0;
        }
        
        if (params.get("redeliveredLimit") instanceof Integer) {
            redeliveredLimit = (Integer) params.get("redeliveredLimit");
            if (redeliveredLimit < 0) redeliveredLimit = 0;
        }
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .put("subscriberName", subscriberName)
                .put("recyclebinName", recyclebinName)
                .put("prefetch", prefetch)
                .put("subscriberLimit", subscriberLimit)
                .put("redeliveredLimit", redeliveredLimit)
                .tags("PubsubHandler.new() parameters")
                .text("PubsubHandler[${pubsubHandlerId}].new() parameters")
                .stringify());
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("PubsubHandler[${pubsubHandlerId}].new() end!")
                .stringify());
    }

    public void publish(String body) {
        publish(body, null);
    }
    
    public void publish(String body, Map<String, Object> opts) {
        publish(body, opts, null);
    }
    
    public void publish(String body, Map<String, Object> opts, String routingKey) {
        publish(OpflowUtil.getBytes(body), opts, routingKey);
    }
    
    public void publish(byte[] body) {
        publish(body, null);
    }
    
    public void publish(byte[] body, Map<String, Object> options) {
        publish(body, options, null);
    }
    
    public void publish(byte[] body, Map<String, Object> options, String routingKey) {
        options = OpflowUtil.ensureNotNull(options);
        
        Object requestId = options.get("requestId");
        if (requestId == null) {
            options.put("requestId", requestId = OpflowUtil.getLogID());
        }
        
        Map<String, Object> override = new HashMap<>();
        if (routingKey != null) {
            override.put("routingKey", routingKey);
        }
        
        OpflowLogTracer logPublish = null;
        if (OpflowLogTracer.has(LOG, "info")) logPublish = logTracer.branch("requestId", requestId);

        if (OpflowLogTracer.has(LOG, "info") && logPublish != null) LOG.info(logPublish
                .put("routingKey", routingKey)
                .text("Request[${requestId}] - PubsubHandler[${pubsubHandlerId}].publish() - routingKey: ${routingKey}")
                .stringify());
        
        engine.produce(body, options, override);
        
        if (OpflowLogTracer.has(LOG, "info") && logPublish != null) LOG.info(logPublish
                .text("Request[${requestId}] - PubsubHandler[${pubsubHandlerId}].publish() request has completed")
                .stringify());
    }
    
    public OpflowEngine.ConsumerInfo subscribe(final OpflowPubsubListener newListener) {
        final String _consumerId = OpflowUtil.getLogID();
        final OpflowLogTracer logSubscribe = logTracer.branch("consumerId", _consumerId);
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logSubscribe
                .text("Consumer[${consumerId}] - PubsubHandler[${pubsubHandlerId}].subscribe() is invoked")
                .stringify());
        
        listener = (listener != null) ? listener : newListener;
        if (listener == null) {
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logSubscribe
                    .text("Consumer[${consumerId}] - subscribe() failed: PubsubListener should not be null")
                    .stringify());
            throw new IllegalArgumentException("PubsubListener should not be null");
        } else if (listener != newListener) {
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logSubscribe
                    .text("Consumer[${consumerId}] - subscribe() failed: supports only single PubsubListener")
                    .stringify());
            throw new OpflowOperationException("PubsubHandler supports only single PubsubListener");
        }
        
        OpflowEngine.ConsumerInfo consumer = engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                Map<String, Object> headers = properties.getHeaders();
                String requestId = OpflowUtil.getRequestId(headers, true);
                OpflowLogTracer logRequest = null;
                if (OpflowLogTracer.has(LOG, "info")) logRequest = logSubscribe.branch("requestId", requestId);
                if (OpflowLogTracer.has(LOG, "info") && logRequest != null) LOG.info(logRequest
                        .text("Request[${requestId}] - Consumer[${consumerId}].subscribe() receives a new request")
                        .stringify());
                try {
                    listener.processMessage(new OpflowMessage(content, headers));
                    if (OpflowLogTracer.has(LOG, "info") && logRequest != null) LOG.info(logRequest
                            .text("Request[${requestId}] - subscribe() request processing has completed")
                            .stringify());
                } catch (Exception exception) {
                    int redeliveredCount = 0;
                    if (headers.get("redeliveredCount") instanceof Integer) {
                        redeliveredCount = (Integer) headers.get("redeliveredCount");
                    }
                    redeliveredCount += 1;
                    headers.put("redeliveredCount", redeliveredCount);
                    
                    AMQP.BasicProperties.Builder propBuilder = copyBasicProperties(properties);
                    AMQP.BasicProperties props = propBuilder.headers(headers).build();
                    
                    if (OpflowLogTracer.has(LOG, "info") && logRequest != null) LOG.info(logRequest
                            .put("redeliveredCount", redeliveredCount)
                            .put("redeliveredLimit", redeliveredLimit)
                            .text("Request[${requestId}] - subscribe() recycling failed request")
                            .stringify());
                    
                    if (redeliveredCount <= redeliveredLimit) {
                        if (OpflowLogTracer.has(LOG, "info") && logRequest != null) LOG.info(logRequest
                                .text("Request[${requestId}] - subscribe() requeue failed request")
                                .stringify());
                        sendToQueue(content, props, subscriberName, channel);
                    } else {
                        if (recyclebinName != null) {
                            sendToQueue(content, props, recyclebinName, channel);
                            if (OpflowLogTracer.has(LOG, "info") && logRequest != null) LOG.info(logRequest
                                    .put("recyclebinName", recyclebinName)
                                    .text("Request[${requestId}] - subscribe() enqueue failed request to recyclebin")
                                    .stringify());
                        } else {
                            if (OpflowLogTracer.has(LOG, "info") && logRequest != null) LOG.info(logRequest
                                    .text("Request[${requestId}] - subscribe() discard failed request (recyclebin not found)")
                                    .stringify());
                        }
                    }
                }
                return true;
            }
        }, OpflowUtil.buildMap(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("consumerId", _consumerId);
                opts.put("autoAck", Boolean.TRUE);
                opts.put("queueName", subscriberName);
                if (prefetch > 0) opts.put("prefetch", prefetch);
                if (subscriberLimit > 0) opts.put("consumerLimit", subscriberLimit);
            }
        }).toMap());
        consumerInfos.add(consumer);
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logSubscribe
                .text("Consumer[${consumerId}] - subscribe() has completed")
                .stringify());
        return consumer;
    }
    
    @Override
    public void close() {
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("PubsubHandler[${pubsubHandlerId}].close()")
                .stringify());
        if (engine != null) {
            for(OpflowEngine.ConsumerInfo consumerInfo:consumerInfos) {
                if (consumerInfo != null) {
                    engine.cancelConsumer(consumerInfo);
                }
            }
            consumerInfos.clear();
            engine.close();
        }
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("PubsubHandler[${pubsubHandlerId}].close() has completed")
                .stringify());
    }
    
    public State check() {
        State state = new State(engine.check());
        return state;
    }
    
    public class State extends OpflowEngine.State {
        public State(OpflowEngine.State superState) {
            super(superState);
        }
    }

    public OpflowExecutor getExecutor() {
        return executor;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    public String getRecyclebinName() {
        return recyclebinName;
    }

    public int getRedeliveredLimit() {
        return redeliveredLimit;
    }
    
    private void sendToQueue(byte[] data, AMQP.BasicProperties replyProps, String queueName, Channel channel) {
        try {
            channel.basicPublish("", queueName, replyProps, data);
        } catch (IOException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    private AMQP.BasicProperties.Builder copyBasicProperties(AMQP.BasicProperties properties) {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        
        if (properties.getAppId() != null) builder.appId(properties.getAppId());
        if (properties.getClusterId() != null) builder.clusterId(properties.getClusterId());
        if (properties.getContentEncoding() != null) builder.contentEncoding(properties.getContentEncoding());
        if (properties.getContentType() != null) builder.contentType(properties.getContentType());
        if (properties.getCorrelationId() != null) builder.correlationId(properties.getCorrelationId());
        if (properties.getDeliveryMode() != null) builder.deliveryMode(properties.getDeliveryMode());
        if (properties.getExpiration() != null) builder.expiration(properties.getExpiration());
        if (properties.getMessageId() != null) builder.messageId(properties.getMessageId());
        if (properties.getPriority() != null) builder.priority(properties.getPriority());
        if (properties.getReplyTo() != null) builder.replyTo(properties.getReplyTo());
        if (properties.getTimestamp() != null) builder.timestamp(properties.getTimestamp());
        if (properties.getType() != null) builder.type(properties.getType());
        if (properties.getUserId() != null) builder.userId(properties.getUserId());
        
        return builder;
    }
}
