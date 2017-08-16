package com.devebot.opflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.exception.OpflowConsumerLimitExceedException;
import com.devebot.opflow.exception.OpflowOperationException;

/**
 *
 * @author drupalex
 */
public class OpflowBroker {

    final Logger logger = LoggerFactory.getLogger(OpflowBroker.class);

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    private String exchangeName;
    private String exchangeType;
    private Boolean exchangeDurable;
    private String routingKey;
    private String[] otherKeys;
    private String applicationId;

    public OpflowBroker(Map<String, Object> params) throws OpflowConstructorException {
        try {
            factory = new ConnectionFactory();

            String uri = (String) params.get("uri");
            if (uri != null) {
                factory.setUri(uri);
            } else {
                String host = (String) params.get("host");
                if (host == null) host = "localhost";
                factory.setHost(host);

                String virtualHost = (String) params.get("virtualHost");
                if (virtualHost != null) {
                    factory.setVirtualHost(virtualHost);
                }

                String username = (String) params.get("username");
                if (username != null) {
                    factory.setUsername(username);
                }

                String password = (String) params.get("password");
                if (password != null) {
                    factory.setPassword(password);
                }
            }

            connection = factory.newConnection();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("newConnection() has been failed, exception: " + exception.getMessage());
            throw new OpflowConstructorException("connection refused, invalid connection parameters", exception);
        }
        
        try {
            if (params.get("exchangeName") instanceof String) {
                exchangeName = (String) params.get("exchangeName");
            }
            
            if (params.get("exchangeType") instanceof String) {
                exchangeType = (String) params.get("exchangeType");
            }
            if (exchangeType == null) exchangeType = "direct";

            if (params.get("exchangeDurable") instanceof Boolean) {
                exchangeDurable = (Boolean) params.get("exchangeDurable");
            }
            if (exchangeDurable == null) exchangeDurable = true;
            
            if (exchangeName != null) {
                getChannel().exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
            }
            
            if (params.get("routingKey") instanceof String) {
                routingKey = (String) params.get("routingKey");
            }
            
            if (params.get("otherKeys") instanceof String[]) {
                otherKeys = (String[])params.get("otherKeys");
            }
            
            if (params.get("applicationId") instanceof String) {
                applicationId = (String) params.get("applicationId");
            }
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("exchangeDeclare has been failed, exception: " + exception.getMessage());
            throw new OpflowConstructorException("exchangeDeclare has been failed", exception);
        }
    }
    
    public void produce(final byte[] content, final AMQP.BasicProperties.Builder propBuilder) {
        produce(content, propBuilder, null);
    }
    
    public void produce(final byte[] content, final AMQP.BasicProperties.Builder propBuilder, final Map<String, Object> override) {
        try {
            String customKey = this.routingKey;
            if (override != null && override.get("routingKey") != null) {
                customKey = (String) override.get("routingKey");
            }
            String appId = this.applicationId;
            if (override != null && override.get("applicationId") != null) {
                appId = (String) override.get("applicationId");
            }
            propBuilder.appId(appId);
            Channel _channel = getChannel();
            if (_channel == null || !_channel.isOpen()) {
                throw new OpflowOperationException("Channel is null or has been closed");
            }
            _channel.basicPublish(this.exchangeName, customKey, propBuilder.build(), content);
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("produce() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    public ConsumerInfo consume(final OpflowListener listener, final Map<String, Object> options) {
        Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        try {
            final Channel _channel;
            
            final Boolean _forceNewChannel = (Boolean) opts.get("forceNewChannel");
            if (!Boolean.FALSE.equals(_forceNewChannel)) {
                _channel = this.connection.createChannel();
            } else {
                _channel = getChannel();
            }
            
            Integer _prefetch = null;
            if (opts.get("prefetch") instanceof Integer) {
                _prefetch = (Integer) opts.get("prefetch");
            }
            if (_prefetch != null && _prefetch > 0) {
                _channel.basicQos(_prefetch);
            }
            
            final String _queueName;
            final boolean _fixedQueue;
            String opts_queueName = (String) opts.get("queueName");
            AMQP.Queue.DeclareOk _declareOk;
            if (opts_queueName != null) {
                _declareOk = _channel.queueDeclare(opts_queueName, true, false, false, null);
                _fixedQueue = true;
            } else {
                _declareOk = _channel.queueDeclare();
                _fixedQueue = false;
            }
            _queueName = _declareOk.getQueue();
            final Integer _consumerLimit = (Integer) opts.get("consumerLimit");
            if (logger.isTraceEnabled()) {
                logger.trace("consume() - consumerCount/consumerLimit: " + _declareOk.getConsumerCount() + "/" + _consumerLimit);
            }
            if (_consumerLimit != null && _consumerLimit > 0) {
                if (_declareOk.getConsumerCount() >= _consumerLimit) {
                    String errorMessage = "consumerLimit exceed: " + _declareOk.getConsumerCount() + "/" + _consumerLimit;
                    if (logger.isErrorEnabled()) logger.error("consume() - " + errorMessage);
                    throw new OpflowConsumerLimitExceedException(errorMessage);
                }
            }
            
            final Boolean _binding = (Boolean) opts.get("binding");
            if (!Boolean.FALSE.equals(_binding) && exchangeName != null) {
                if (routingKey != null) {
                    bindExchange(_channel, exchangeName, _queueName, routingKey);
                }
                if (otherKeys != null) {
                    bindExchange(_channel, exchangeName, _queueName, otherKeys);
                }
            }
            
            final String _replyToName;
            String opts_replyToName = (String) opts.get("replyTo");
            if (opts_replyToName != null) {
                _replyToName = _channel.queueDeclarePassive(opts_replyToName).getQueue();
            } else {
                _replyToName = null;
            }
            
            final Boolean _autoAck;
            if (opts.get("autoAck") != null && opts.get("autoAck") instanceof Boolean) {
                _autoAck = (Boolean) opts.get("autoAck");
            } else {
                _autoAck = Boolean.TRUE;
            }
            
            final Consumer _consumer = new DefaultConsumer(_channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String requestID = OpflowUtil.getRequestId(properties.getHeaders(), false);

                    if (logger.isInfoEnabled()) {
                        logger.info("Request["+requestID+"] / DeliveryTag["+envelope.getDeliveryTag()+"] / ConsumerTag["+consumerTag+"]");
                    }

                    if (logger.isTraceEnabled()) {
                        if (body.length <= 4096) {
                            logger.trace("Request[" + requestID + "] - Message: " + new String(body, "UTF-8"));
                        } else {
                            logger.trace("Request[" + requestID + "] - Message size too large (>4KB): " + body.length);
                        }
                    }
                    
                    try {
                        if (applicationId == null || applicationId.equals(properties.getAppId())) {
                            if (logger.isTraceEnabled()) {
                                logger.trace(MessageFormat.format("Request[{0}] invoke listener.processMessage()", new Object[] {
                                    requestID
                                }));
                            }

                            boolean captured = listener.processMessage(body, properties, _replyToName, _channel, consumerTag);

                            if (captured) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("Request[" + requestID + "] has finished successfully");
                                }
                            } else {
                                if (logger.isInfoEnabled()) {
                                    logger.info("Request[" + requestID + "] has not matched the criteria, skipped");
                                }
                            }
                            
                            if (logger.isTraceEnabled()) {
                                logger.trace(MessageFormat.format("Request[{0}] invoke Ack({1}, false)) / ConsumerTag[{2}]", new Object[] {
                                    requestID, envelope.getDeliveryTag(), consumerTag
                                }));
                            }

                            if (!_autoAck) _channel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            if (logger.isInfoEnabled()) {
                                logger.info(MessageFormat.format("Request[{0}]/AppId:{1} - but received AppId:{2}, rejected", new Object[] {
                                    requestID, applicationId, properties.getAppId()
                                }));
                            }
                            if (!_autoAck) _channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                    } catch (Exception ex) {
                        // catch ALL of Error here: don't let it harm our service/close the channel
                        if (logger.isErrorEnabled()) {
                            logger.error(MessageFormat.format("Request[{0}]/DeliveryTag[{1}]/ConsumerTag[{2}] has been failed. " +
                                    "Exception.Class: {3} / message: {4}. Service still alive", new Object[] {
                                requestID, envelope.getDeliveryTag(), consumerTag, ex.getClass().getName(), ex.getMessage()
                            }));
                        }
                        if (_autoAck) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Request[" + requestID + "] has been failed. AutoAck => request is rejected");
                            }
                        } else {
                            _channel.basicNack(envelope.getDeliveryTag(), false, true);
                            if (logger.isInfoEnabled()) {
                                logger.info("Request[" + requestID + "] has been failed. No AutoAck => request is requeued");
                            }
                        }
                    }
                }
                
                @Override
                public void handleCancelOk(String consumerTag) {
                    if (!Boolean.FALSE.equals(_forceNewChannel)) {
                        try {
                            if (_channel != null && _channel.isOpen()) _channel.close();
                        } catch (IOException ex) {
                            if (logger.isErrorEnabled()) {
                                logger.error(MessageFormat.format("ConsumerTag[{0}] handleCancelOk failed, IOException: {1}", new Object[] {
                                    consumerTag, ex.getMessage()
                                }));
                            }
                        } catch (TimeoutException ex) {
                            if (logger.isErrorEnabled()) {
                                logger.error(MessageFormat.format("ConsumerTag[{0}] handleCancelOk failed, TimeoutException: {1}", new Object[] {
                                    consumerTag, ex.getMessage()
                                }));
                            }
                        }
                    }
                }
                
                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    if (logger.isInfoEnabled()) {
                        logger.info(MessageFormat.format("ConsumerTag[{0}] handle shutdown signal", new Object[] {
                            consumerTag
                        }));
                    }
                }
            };
            
            final String _consumerTag = _channel.basicConsume(_queueName, _autoAck, _consumer);
            
            _channel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (logger.isInfoEnabled()) {
                        logger.info(MessageFormat.format("Channel[{0}] contains Queue[{1}]/ConsumerTag[{2}] has been shutdown", new Object[] {
                            _channel.getChannelNumber(), _queueName, _consumerTag
                        }));
                    }
                }
            });
            
            if (logger.isInfoEnabled()) {
                logger.info("[*] Consume Channel[" + _channel.getChannelNumber() + "]/Queue[" + _queueName + "] -> consumerTag: " + _consumerTag);
            }
            return new ConsumerInfo(_channel, _queueName, _fixedQueue, _consumer, _consumerTag);
        } catch(IOException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    public void cancelConsumer(OpflowBroker.ConsumerInfo consumerInfo) {
        if (consumerInfo == null) return;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Queue[" + consumerInfo.getQueueName() + "]/ConsumerTag[" + consumerInfo.getConsumerTag() + "] will be cancelled");
            }
            consumerInfo.getChannel().basicCancel(consumerInfo.getConsumerTag());
            if (logger.isDebugEnabled()) {
                logger.debug("Queue[" + consumerInfo.getQueueName() + "]/ConsumerTag[" + consumerInfo.getConsumerTag() + "] has been cancelled");
            }
        } catch (IOException ex) {
            if (logger.isErrorEnabled()) {
                logger.error("cancel consumer[" + consumerInfo.getConsumerTag() + "] failed, IOException: " + ex.getMessage());
            }
        }
    }
    
    public class ConsumerInfo {
        private final Channel channel;
        private final String queueName;
        private final boolean fixedQueue;
        private final Consumer consumer;
        private final String consumerTag;
        
        public ConsumerInfo(Channel channel, String queueName, boolean fixedQueue, Consumer consumer, String consumerTag) {
            this.channel = channel;
            this.queueName = queueName;
            this.fixedQueue = fixedQueue;
            this.consumer = consumer;
            this.consumerTag = consumerTag;
        }

        public Channel getChannel() {
            return channel;
        }

        public String getQueueName() {
            return queueName;
        }

        public boolean isFixedQueue() {
            return fixedQueue;
        }
        
        public Consumer getConsumer() {
            return consumer;
        }

        public String getConsumerTag() {
            return consumerTag;
        }
    }
    
    public static class State {
        public static final int CONNECTION_NEW = 0;
        public static final int CONNECTION_OPENED = 1;
        public static final int CONNECTION_CLOSED = 2;
        private final int[] CONNECTION_STATES =  new int[] {
            CONNECTION_NEW, CONNECTION_OPENED, CONNECTION_CLOSED
        };
        
        private int connectionState = -1;
        
        public int getConnectionState() {
            return connectionState;
        }
        
        public State(State state) {
            this.connectionState = state.connectionState;
        }
        
        private State(int connectionState) {
            for (int i=0; i<CONNECTION_STATES.length; i++) {
                if (CONNECTION_STATES[i] == connectionState) {
                    this.connectionState = connectionState;
                    break;
                }
            }
            if (this.connectionState < 0) this.connectionState = CONNECTION_NEW;
        }
    }
    
    public State check() {
        int conn = connection.isOpen() ? State.CONNECTION_OPENED : State.CONNECTION_CLOSED;
        State state = new State(conn);
        return state;
    }
    
    /**
     * Close this broker.
     *
     * @throws OpflowOperationException if an error is encountered
     */
    public void close() {
        try {
            if (logger.isInfoEnabled()) logger.info("[*] Cancel consumers, close channels, close connection.");
            if (channel != null && channel.isOpen()) channel.close();
            if (connection != null && connection.isOpen()) connection.close();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("close() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    private Channel getChannel() throws IOException {
        if (channel == null) {
            try {
                channel = connection.createChannel();
                channel.addShutdownListener(new ShutdownListener() {
                    @Override
                    public void shutdownCompleted(ShutdownSignalException sse) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Main channel[" + channel.getChannelNumber() + "] has been shutdown");
                        }
                    }
                });
            } catch (IOException exception) {
                if (logger.isErrorEnabled()) logger.error("getChannel() has been failed, exception: " + exception.getMessage());
                throw exception;
            }
        }
        return channel;
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String _routingKey) throws IOException {
        bindExchange(_channel, _exchangeName, _queueName, new String[] { _routingKey });
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String[] keys) throws IOException {
        _channel.exchangeDeclarePassive(_exchangeName);
        _channel.queueDeclarePassive(_queueName);
        for (String _routingKey : keys) {
            _channel.queueBind(_queueName, _exchangeName, _routingKey);
            if (logger.isTraceEnabled()) {
                logger.trace(MessageFormat.format("Exchange[{0}] binded to Queue[{1}] with key[{2}]", new Object[] {
                    _exchangeName, _queueName, _routingKey
                }));
            }
        }
    }
}