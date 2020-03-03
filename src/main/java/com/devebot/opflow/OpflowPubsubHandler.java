package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.exception.OpflowRestrictionException;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedListener;
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
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPubsubHandler.class);

    private final String componentId;
    private final OpflowLogTracer logTracer;
    
    private final OpflowRestrictor.Valve restrictor;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;

    private final String subscriberName;
    private final String recyclebinName;
    private final List<OpflowEngine.ConsumerInfo> consumerInfos = new LinkedList<>();
    private int prefetchCount = 0;
    private int subscriberLimit = 0;
    private int redeliveredLimit = 0;
    private OpflowPubsubListener listener;

    private final boolean autorun;

    public OpflowPubsubHandler(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowObjectTree.ensureNonNull(params);
        
        componentId = OpflowUtil.getOptionField(params, CONST.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("pubsubHandlerId", componentId);
        
        restrictor = new OpflowRestrictor.Valve();

        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("PubsubHandler[${pubsubHandlerId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put(CONST.COMPONENT_ID, componentId);
        brokerParams.put("mode", "pubsub");
        brokerParams.put("exchangeType", "direct");
        
        subscriberName = (String) params.get("subscriberName");
        recyclebinName = (String) params.get("recyclebinName");
        
        if (subscriberName != null && recyclebinName != null && subscriberName.equals(recyclebinName)) {
            throw new OpflowBootstrapException("subscriberName should be different with recyclebinName");
        }
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);

        engine.setProducingBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) throws IOException {
                if (restrictor != null) {
                    restrictor.block();
                }
            }

            @Override
            public void handleUnblocked() throws IOException {
                if (restrictor != null) {
                    restrictor.unblock();
                }
            }
        });

        if (subscriberName != null) {
            executor.assertQueue(subscriberName);
        }
        
        if (recyclebinName != null) {
            executor.assertQueue(recyclebinName);
        }
        
        if (params.get("prefetchCount") instanceof Integer) {
            prefetchCount = (Integer) params.get("prefetchCount");
            if (prefetchCount < 0) prefetchCount = 0;
        }
        
        if (params.get("subscriberLimit") instanceof Integer) {
            subscriberLimit = (Integer) params.get("subscriberLimit");
            if (subscriberLimit < 0) subscriberLimit = 0;
        }
        
        if (params.get("redeliveredLimit") instanceof Integer) {
            redeliveredLimit = (Integer) params.get("redeliveredLimit");
            if (redeliveredLimit < 0) redeliveredLimit = 0;
        }
        
        if (params.get("autorun") instanceof Boolean) {
            autorun = (Boolean) params.get("autorun");
        } else {
            autorun = false;
        }
        
        if (autorun) {
            this.serve();
        }

        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("autorun", autorun)
                .put("subscriberName", subscriberName)
                .put("recyclebinName", recyclebinName)
                .put("prefetchCount", prefetchCount)
                .put("subscriberLimit", subscriberLimit)
                .put("redeliveredLimit", redeliveredLimit)
                .tags("PubsubHandler.new() parameters")
                .text("PubsubHandler[${pubsubHandlerId}].new() parameters")
                .stringify());
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("PubsubHandler[${pubsubHandlerId}].new() end!")
                .stringify());
    }

    public void publish(String body) {
        publish(body, null, null);
    }
    
    public void publish(String body, Map<String, Object> opts) {
        publish(body, opts, null);
    }
    
    public void publish(String body, Map<String, Object> opts, String routingKey) {
        publish(OpflowUtil.getBytes(body), opts, routingKey);
    }
    
    public void publish(byte[] body) {
        publish(body, null, null);
    }
    
    public void publish(byte[] body, Map<String, Object> headers) {
        publish(body, headers, null);
    }
    
    public void publish(final byte[] body, final Map<String, Object> headers, final String routingKey) {
        if (restrictor == null) {
            _publish(body, headers, routingKey);
            return;
        }
        try {
            restrictor.filter(new OpflowRestrictor.Action<Object>() {
                @Override
                public Object process() throws Throwable {
                    _publish(body, headers, routingKey);
                    return null;
                }
            });
        }
        catch (OpflowOperationException opflowException) {
            throw opflowException;
        }
        catch (Throwable e) {
            throw new OpflowRestrictionException(e);
        }
    }
    
    private void _publish(byte[] body, Map<String, Object> headers, String routingKey) {
        headers = OpflowObjectTree.ensureNonNull(headers);
        
        String routineId = OpflowUtil.getRoutineId(headers);
        String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);
        
        OpflowLogTracer logPublish = null;
        if (logTracer.ready(LOG, Level.INFO)) {
            logPublish = logTracer.branch(CONST.REQUEST_TIME, routineTimestamp).branch(CONST.REQUEST_ID, routineId);
        }
        
        Map<String, Object> override = new HashMap<>();
        if (routingKey != null) {
            override.put(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY, routingKey);
            if (logPublish != null && logPublish.ready(LOG, Level.INFO)) LOG.info(logPublish
                    .put("routingKey", routingKey)
                    .text("Request[${requestId}][${requestTime}] - PubsubHandler[${pubsubHandlerId}].publish() with overridden routingKey: ${routingKey}")
                    .stringify());
        } else {
            if (logPublish != null && logPublish.ready(LOG, Level.INFO)) LOG.info(logPublish
                    .text("Request[${requestId}][${requestTime}] - PubsubHandler[${pubsubHandlerId}].publish()")
                    .stringify());
        }
        
        engine.produce(body, headers, override);
        
        if (logPublish != null && logPublish.ready(LOG, Level.INFO)) LOG.info(logPublish
                .text("Request[${requestId}][${requestTime}] - PubsubHandler[${pubsubHandlerId}].publish() request has enqueued")
                .stringify());
    }
    
    public OpflowEngine.ConsumerInfo subscribe(final OpflowPubsubListener newListener) {
        final String _consumerId = OpflowUUID.getBase64ID();
        final OpflowLogTracer logSubscribe = logTracer.branch("consumerId", _consumerId);
        if (logSubscribe.ready(LOG, Level.INFO)) LOG.info(logSubscribe
                .text("Consumer[${consumerId}] - PubsubHandler[${pubsubHandlerId}].subscribe() is invoked")
                .stringify());
        
        listener = (listener != null) ? listener : newListener;
        if (listener == null) {
            if (logSubscribe.ready(LOG, Level.INFO)) LOG.info(logSubscribe
                    .text("Consumer[${consumerId}] - subscribe() failed: PubsubListener should not be null")
                    .stringify());
            throw new IllegalArgumentException("PubsubListener should not be null");
        } else if (listener != newListener) {
            if (logSubscribe.ready(LOG, Level.INFO)) LOG.info(logSubscribe
                    .text("Consumer[${consumerId}] - subscribe() failed: supports only single PubsubListener")
                    .stringify());
            throw new OpflowOperationException("PubsubHandler supports only single PubsubListener");
        }
        
        OpflowEngine.ConsumerInfo consumer = engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(
                    byte[] content,
                    AMQP.BasicProperties properties,
                    String queueName,
                    Channel channel,
                    String consumerTag,
                    Map<String, String> extras
            ) throws IOException {
                Map<String, Object> headers = properties.getHeaders();
                String routineId = OpflowUtil.getRoutineId(headers, false);
                String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers, false);
                OpflowLogTracer reqTracer = null;
                if (logSubscribe.ready(LOG, Level.INFO)) {
                    reqTracer = logSubscribe.branch(CONST.REQUEST_TIME, routineTimestamp).branch(CONST.REQUEST_ID, routineId);
                }
                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                        .text("Request[${requestId}][${requestTime}] - Consumer[${consumerId}].subscribe() receives a new request")
                        .stringify());
                try {
                    listener.processMessage(new OpflowMessage(content, headers));
                    if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .text("Request[${requestId}][${requestTime}] - subscribe() request processing has completed")
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
                    
                    if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .put("redeliveredCount", redeliveredCount)
                            .put("redeliveredLimit", redeliveredLimit)
                            .text("Request[${requestId}][${requestTime}] - subscribe() recycling failed request")
                            .stringify());
                    
                    if (redeliveredCount <= redeliveredLimit) {
                        if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                .text("Request[${requestId}][${requestTime}] - subscribe() requeue failed request")
                                .stringify());
                        sendToQueue(content, props, subscriberName, channel);
                    } else {
                        if (recyclebinName != null) {
                            sendToQueue(content, props, recyclebinName, channel);
                            if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                    .put("recyclebinName", recyclebinName)
                                    .text("Request[${requestId}][${requestTime}] - subscribe() enqueue failed request to recyclebin")
                                    .stringify());
                        } else {
                            if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                    .text("Request[${requestId}][${requestTime}] - subscribe() discard failed request (recyclebin not found)")
                                    .stringify());
                        }
                    }
                }
                return true;
            }
        }, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_ID, _consumerId);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_AUTO_ACK, Boolean.TRUE);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME, subscriberName);
                if (prefetchCount > 0) opts.put(OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT, prefetchCount);
                if (subscriberLimit > 0) opts.put(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_LIMIT, subscriberLimit);
            }
        }).toMap());
        consumerInfos.add(consumer);
        if (logSubscribe.ready(LOG, Level.INFO)) LOG.info(logSubscribe
                .text("Consumer[${consumerId}] - subscribe() has completed")
                .stringify());
        return consumer;
    }
    
    public final void serve() {
        if (restrictor != null) {
            restrictor.unblock();
        }
    }
    
    @Override
    public void close() {
        if (restrictor != null) {
            restrictor.block();
        }
        try {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("PubsubHandler[${pubsubHandlerId}][${instanceId}].close()")
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
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("PubsubHandler[${pubsubHandlerId}][${instanceId}].close() end!")
                    .stringify());
        }
        finally {
            if (autorun) {
                if (restrictor != null) {
                    restrictor.unblock();
                }
                if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                        .text("PubsubHandler[${pubsubHandlerId}].close() - lock is released")
                        .stringify());
            }
        }
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
