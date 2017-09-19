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
public class OpflowPubsubHandler {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPubsubHandler.class);
    private final OpflowLogTracer logTracer;

    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    private final String subscriberName;
    private final String recyclebinName;
    private final List<OpflowEngine.ConsumerInfo> consumerInfos = new LinkedList<OpflowEngine.ConsumerInfo>();
    private int prefetch = 0;
    private int subscriberLimit = 0;
    private int redeliveredLimit = 0;
    private OpflowPubsubListener listener;

    public OpflowPubsubHandler(Map<String, Object> params) throws OpflowBootstrapException {
        final String pubsubHandlerId = OpflowUtil.getOptionField(params, "pubsubHandlerId", true);
        logTracer = OpflowLogTracer.ROOT.branch("pubsubHandlerId", pubsubHandlerId);
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "PubsubHandler.new()")
                .toString());
        
        Map<String, Object> brokerParams = new HashMap<String, Object>();
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
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("subscriberName", subscriberName)
                .put("recyclebinName", recyclebinName)
                .put("prefetch", prefetch)
                .put("subscriberLimit", subscriberLimit)
                .put("redeliveredLimit", redeliveredLimit)
                .put("message", "PubsubHandler.new() parameters")
                .toString());
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "PubsubHandler.new() end!")
                .toString());
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
        AMQP.BasicProperties.Builder propBuilder = new AMQP.BasicProperties.Builder();
        
        if (options == null) {
            options = new HashMap<String, Object>();
        }
        Object requestId = options.get("requestId");
        if (requestId == null) {
            options.put("requestId", requestId = OpflowUtil.getUUID());
        }
        
        Map<String, Object> override = new HashMap<String, Object>();
        if (routingKey != null) {
            override.put("routingKey", routingKey);
        }
        
        OpflowLogTracer logPublish = null;
        if (LOG.isInfoEnabled()) logPublish = logTracer.branch("requestId", requestId);

        if (LOG.isInfoEnabled() && logPublish != null) LOG.info(logPublish
                .put("routingKey", routingKey)
                .put("message", "publish() - Request is produced with overriden routingKey")
                .toString());
        
        engine.produce(body, options, override);
        
        if (LOG.isInfoEnabled() && logPublish != null) LOG.info(logPublish.reset()
                .put("message", "publish() - Request has completed")
                .toString());
    }
    
    public OpflowEngine.ConsumerInfo subscribe(final OpflowPubsubListener newListener) {
        final String _consumerId = OpflowUtil.getUUID();
        final OpflowLogTracer logSubscribe = logTracer.branch("consumerId", _consumerId);
        if (LOG.isInfoEnabled()) LOG.info(logSubscribe
                .put("message", "subscribe() is invoked")
                .toString());
        
        listener = (listener != null) ? listener : newListener;
        if (listener == null) {
            if (LOG.isInfoEnabled()) LOG.info(logSubscribe
                    .put("message", "subscribe() - PubsubListener should not be null")
                    .toString());
            throw new IllegalArgumentException("PubsubListener should not be null");
        } else if (listener != newListener) {
            if (LOG.isInfoEnabled()) LOG.info(logSubscribe
                    .put("message", "subscribe() - PubsubHandler supports only single PubsubListener")
                    .toString());
            throw new OpflowOperationException("PubsubHandler supports only single PubsubListener");
        }
        
        OpflowEngine.ConsumerInfo consumer = engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                Map<String, Object> headers = properties.getHeaders();
                String requestId = OpflowUtil.getRequestId(headers, true);
                OpflowLogTracer logRequest = null;
                if (LOG.isInfoEnabled()) logRequest = logSubscribe.branch("requestId", requestId);
                if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest
                        .put("message", "subscribe() - receives a new request")
                        .toString());
                try {
                    listener.processMessage(new OpflowMessage(content, headers));
                    if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest.reset()
                            .put("message", "subscribe() - request processing has completed")
                            .toString());
                } catch (Exception exception) {
                    int redeliveredCount = 0;
                    if (headers.get("redeliveredCount") instanceof Integer) {
                        redeliveredCount = (Integer) headers.get("redeliveredCount");
                    }
                    redeliveredCount += 1;
                    headers.put("redeliveredCount", redeliveredCount);
                    
                    AMQP.BasicProperties.Builder propBuilder = copyBasicProperties(properties);
                    AMQP.BasicProperties props = propBuilder.headers(headers).build();
                    
                    if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest.reset()
                            .put("redeliveredCount", redeliveredCount)
                            .put("redeliveredLimit", redeliveredLimit)
                            .put("message", "subscribe() - recycling failed request")
                            .toString());
                    
                    if (redeliveredCount <= redeliveredLimit) {
                        if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest.reset()
                                .put("message", "subscribe() - requeue failed request")
                                .toString());
                        sendToQueue(content, props, subscriberName, channel);
                    } else {
                        if (recyclebinName != null) {
                            sendToQueue(content, props, recyclebinName, channel);
                            if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest.reset()
                                    .put("recyclebinName", recyclebinName)
                                    .put("message", "subscribe() - enqueue failed request to recyclebin")
                                    .toString());
                        } else {
                            if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest.reset()
                                    .put("message", "subscribe() - recyclebin not found, discard failed request")
                                    .toString());
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
        if (LOG.isInfoEnabled()) LOG.info(logSubscribe.reset()
                .put("message", "subscribe() has completed")
                .toString());
        return consumer;
    }
    
    public void close() {
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "close()")
                .toString());
        if (engine != null) {
            for(OpflowEngine.ConsumerInfo consumerInfo:consumerInfos) {
                if (consumerInfo != null) {
                    engine.cancelConsumer(consumerInfo);
                }
            }
            consumerInfos.clear();
            engine.close();
        }
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "close() has completed")
                .toString());
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
