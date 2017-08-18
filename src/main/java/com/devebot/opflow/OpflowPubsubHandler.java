package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubHandler {
    
    final Logger logger = LoggerFactory.getLogger(OpflowPubsubHandler.class);

    private final OpflowBroker broker;
    private final OpflowExecutor executor;
    private final String subscriberName;
    private final String recyclebinName;
    private int redeliveredLimit = 0;

    public OpflowPubsubHandler(Map<String, Object> params) throws OpflowConstructorException {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        brokerParams.put("mode", "pubsub");
        brokerParams.put("uri", params.get("uri"));
        brokerParams.put("exchangeName", params.get("exchangeName"));
        brokerParams.put("exchangeType", "direct");
        brokerParams.put("routingKey", params.get("routingKey"));
        if (params.get("otherKeys") instanceof String) {
            brokerParams.put("otherKeys", OpflowUtil.splitByComma((String)params.get("otherKeys")));
        }
        brokerParams.put("applicationId", params.get("applicationId"));
        broker = new OpflowBroker(brokerParams);
        executor = new OpflowExecutor(broker);
        
        subscriberName = (String) params.get("subscriberName");
        if (subscriberName == null) {
            throw new OpflowConstructorException("subscriberName must not be null");
        } else {
            executor.assertQueue(subscriberName);
        }
        
        recyclebinName = (String) params.get("recyclebinName");
        if (recyclebinName != null) {
            executor.assertQueue(recyclebinName);
            if (params.get("redeliveredLimit") instanceof Integer) {
                redeliveredLimit = (Integer) params.get("redeliveredLimit");
                if (redeliveredLimit < 0) redeliveredLimit = 0;
            }
        }
    }

    public void publish(String data) {
        publish(data, null);
    }
    
    public void publish(String data, Map<String, Object> opts) {
        publish(data, opts, null);
    }
    
    public void publish(String data, Map<String, Object> opts, String routingKey) {
        publish(OpflowUtil.getBytes(data), opts, routingKey);
    }
    
    public void publish(byte[] data) {
        publish(data, null);
    }
    
    public void publish(byte[] data, Map<String, Object> opts) {
        publish(data, opts, null);
    }
    
    public void publish(byte[] data, Map<String, Object> opts, String routingKey) {
        AMQP.BasicProperties.Builder propBuilder = new AMQP.BasicProperties.Builder();
        
        if (opts == null) {
            opts = new HashMap<String, Object>();
        }
        if (opts.get("requestId") == null) {
            opts.put("requestId", OpflowUtil.getUUID());
        }
        propBuilder.headers(opts);
        
        Map<String, Object> override = new HashMap<String, Object>();
        if (routingKey != null) {
            override.put("routingKey", routingKey);
        }
        broker.produce(data, propBuilder, override);
    }
    
    public OpflowBroker.ConsumerInfo subscribe(final OpflowPubsubListener listener) {
        return broker.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                try {
                    listener.processMessage(new OpflowMessage(content, properties.getHeaders()));
                } catch (Exception exception) {
                    Map<String, Object> headers = properties.getHeaders();
                    
                    int redeliveredCount = 0;
                    if (headers.get("redeliveredCount") instanceof Integer) {
                        redeliveredCount = (Integer) headers.get("redeliveredCount");
                    }
                    redeliveredCount += 1;
                    headers.put("redeliveredCount", redeliveredCount);
                    
                    AMQP.BasicProperties.Builder propBuilder = copyBasicProperties(properties);
                    AMQP.BasicProperties props = propBuilder.headers(headers).build();
                    
                    if (redeliveredCount <= redeliveredLimit) {
                        sendToQueue(content, props, subscriberName, channel);
                    } else {
                        if (recyclebinName != null) {
                            sendToQueue(content, props, recyclebinName, channel);
                        }
                    }
                }
                return true;
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("autoAck", Boolean.FALSE);
                opts.put("queueName", subscriberName);
            }
        }));
    }
    
    public void close() {
        if (broker != null) {
            broker.close();
        }
    }
    
    public State check() {
        State state = new State(broker.check());
        return state;
    }
    
    public class State extends OpflowBroker.State {
        public State(OpflowBroker.State superState) {
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
