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
import java.util.HashMap;
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
    private String routingKey;
    private String[] otherKeys;

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

            exchangeName = (String) params.get("exchangeName");
            exchangeType = (String) params.get("exchangeType");
            
            if (exchangeType == null) exchangeType = "direct";

            if (exchangeName != null && exchangeType != null) {
                getChannel().exchangeDeclare(exchangeName, exchangeType, true);
            }
            
            if (params.get("routingKey") instanceof String) {
                routingKey = (String) params.get("routingKey");
            }
            
            if (params.get("otherKeys") instanceof String[]) {
                otherKeys = (String[])params.get("otherKeys");
            }
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("new OpflowBroker has been failed, exception: " + exception.getMessage());
            throw new OpflowConstructorException(exception);
        }
    }

    public void produce(final byte[] content, final AMQP.BasicProperties props, final Map<String, Object> override) {
        try {
            String customKey = this.routingKey;
            if (override != null && override.get("routingKey") != null) {
                customKey = (String) override.get("routingKey");
            }
            getChannel().basicPublish(this.exchangeName, customKey, props, content);
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
            
            final Consumer _consumer = new DefaultConsumer(_channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String requestID = OpflowUtil.getRequestID(properties.getHeaders());

                    if (logger.isInfoEnabled()) {
                        logger.info("Request["+requestID+"] / DeliveryTag["+envelope.getDeliveryTag()+"] / ConsumerTag["+consumerTag+"]");
                    }

                    if (logger.isTraceEnabled()) {
                        if (body.length <= 4*1024) {
                            logger.trace("Request[" + requestID + "] - Message: " + new String(body, "UTF-8"));
                        } else {
                            logger.trace("Request[" + requestID + "] - Message size too large (>4KB): " + body.length);
                        }
                    }

                    if (logger.isTraceEnabled()) logger.trace(MessageFormat.format("Request[{0}] invoke listener.processMessage()", new Object[] {
                        requestID
                    }));
                    listener.processMessage(body, properties, _replyToName, _channel, consumerTag);

                    if (logger.isTraceEnabled()) {
                        logger.trace(MessageFormat.format("Request[{0}] invoke Ack({1}, false)) / ConsumerTag[{2}]", new Object[] {
                            requestID, envelope.getDeliveryTag(), consumerTag
                        }));
                    }
                    _channel.basicAck(envelope.getDeliveryTag(), false);

                    if (logger.isInfoEnabled()) {
                        logger.info("Request[" + requestID + "] has finished successfully");
                    }
                }
                
                @Override
                public void handleCancelOk(String consumerTag) {
                    if (!Boolean.FALSE.equals(_forceNewChannel)) {
                        try {
                            _channel.close();
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
            
            final String _consumerTag = _channel.basicConsume(_queueName, false, _consumer);
                        
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
    
    /**
     * Close this broker.
     *
     * @throws OpflowOperationException if an error is encountered
     */
    public void close() {
        try {
            if (logger.isInfoEnabled()) logger.info("[*] Cancel consumers, close channels, close connection.");
            if (channel != null) channel.close();
            if (connection != null) connection.close();
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
        bindExchange(_channel, _exchangeName, _queueName, keys, null);
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String[] keys, Map<String, Object> bindingArgs) throws IOException {
        _channel.exchangeDeclarePassive(_exchangeName);
        _channel.queueDeclarePassive(_queueName);
        if (bindingArgs == null) bindingArgs = new HashMap<String, Object>();
        for (String _routingKey : keys) {
            _channel.queueBind(_queueName, _exchangeName, _routingKey, bindingArgs);
            if (logger.isTraceEnabled()) {
                logger.trace(MessageFormat.format("Exchange[{0}] binded to Queue[{1}] with key[{2}]", new Object[] {
                    _exchangeName, _queueName, _routingKey
                }));
            }
        }
    }
}