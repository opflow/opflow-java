package net.acegik.jsondataflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.acegik.jsondataflow.exception.OpflowOperationException;

/**
 *
 * @author drupalex
 */
public class OpflowEngine {

    final Logger logger = LoggerFactory.getLogger(OpflowEngine.class);

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;

    private String exchangeName;
    private String exchangeType;
    private String routingKey;

    private Consumer operator;
    private String operatorTag;
    private String operatorName;
    
    private Consumer feedback;
    private String feedbackTag;
    private String feedbackName;

    public OpflowEngine(Map<String, Object> params) throws Exception {
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
        channel = connection.createChannel();

        String mode = (String) params.get("mode");
        if ("rpc.master".equals(mode)) {
            channel.basicQos(1);
        }

        exchangeName = (String) params.get("exchangeName");
        exchangeType = (String) params.get("exchangeType");
        routingKey = (String) params.get("routingKey");
        
        if (exchangeType == null) exchangeType = "direct";

        if (exchangeName != null && exchangeType != null) {
            channel.exchangeDeclare(exchangeName, exchangeType, true);
        }
    }

    public void produce(final byte[] content, final AMQP.BasicProperties props, final Map<String, Object> override) {
        try {
            String customKey = this.routingKey;
            if (override != null && override.get("routingKey") != null) {
                customKey = (String) override.get("routingKey");
            }
            channel.basicPublish(this.exchangeName, customKey, props, content);
        } catch (IOException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public ConsumerInfo consume(final OpflowListener listener, final Map<String, Object> options) {
        Map<String, Object> opts = OpflowUtil.assertNotNull(options);
        try {
            final Channel _channel;
            
            Boolean _forceNewChannel = null;
            if (opts.get("forceNewChannel") instanceof Boolean) {
                _forceNewChannel = (Boolean) opts.get("forceNewChannel");
            }
            if (!Boolean.FALSE.equals(_forceNewChannel)) {
                _channel = this.connection.createChannel();
            } else {
                _channel = channel;
            }
            
            Integer _prefetch = null;
            if (opts.get("prefetch") instanceof Integer) {
                _prefetch = (Integer) opts.get("prefetch");
            }
            if (_prefetch != null && _prefetch > 0) {
                channel.basicQos(_prefetch);
            }
            
            final String _queueName;
            String opts_queueName = (String) opts.get("queueName");
            if (opts_queueName != null) {
                _queueName = _channel.queueDeclare(opts_queueName, true, false, false, null).getQueue();
            } else {
                _queueName = _channel.queueDeclare().getQueue();
            }
            
            final Boolean _binding = (Boolean) opts.get("binding");
            if (!Boolean.FALSE.equals(_binding) && exchangeName != null && routingKey != null) {
                _channel.exchangeDeclarePassive(exchangeName);
                Map<String, Object> _bindingArgs = (Map<String, Object>) opts.get("bindingArgs");
                if (_bindingArgs == null) _bindingArgs = new HashMap<String, Object>();
                _channel.queueBind(_queueName, exchangeName, routingKey, _bindingArgs);
                if (logger.isTraceEnabled()) {
                    logger.trace(MessageFormat.format("Exchange[{0}] binded to Queue[{1}] with routingKey[{2}]", new Object[] {
                        exchangeName, _queueName, routingKey
                    }));
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
                    String requestID = getRequestID(properties.getHeaders());

                    if (logger.isInfoEnabled()) {
                        logger.info("Request["+requestID+"] / DeliveryTag["+envelope.getDeliveryTag()+"] / ConsumerTag["+consumerTag+"]");
                    }

                    if (logger.isTraceEnabled()) {
                        if (body.length < 4*1024) {
                            logger.trace("Request[" + requestID + "] - Message: " + new String(body, "UTF-8"));
                        } else {
                            logger.trace("Request[" + requestID + "] - Message size too large: " + body.length);
                        }
                    }

                    if (logger.isTraceEnabled()) logger.trace(MessageFormat.format("Request[{0}] invoke listener.processMessage()", new Object[] {
                        requestID
                    }));
                    listener.processMessage(body, properties, _replyToName, _channel);

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
            };
            String _consumerTag = _channel.basicConsume(_queueName, false, _consumer);
            if (logger.isInfoEnabled()) {
                logger.info("[*] Consume queue[" + _queueName + "] -> consumerTag: " + _consumerTag);
            }
            return new ConsumerInfo(_channel, _queueName, _consumer, _consumerTag);
        } catch(IOException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    public class ConsumerInfo {
        private final Channel channel;
        private final String queueName;
        private final Consumer consumer;
        private final String consumerTag;
        
        public ConsumerInfo(Channel channel, String queueName, Consumer consumer, String consumerTag) {
            this.channel = channel;
            this.queueName = queueName;
            this.consumer = consumer;
            this.consumerTag = consumerTag;
        }

        public Channel getChannel() {
            return channel;
        }

        public String getQueueName() {
            return queueName;
        }

        public Consumer getConsumer() {
            return consumer;
        }

        public String getConsumerTag() {
            return consumerTag;
        }
    }
    
    public void close() {
        try {
            if (logger.isInfoEnabled()) logger.info("[*] Cancel consumer; close channel, connection.");
            if (operatorTag != null) {
                channel.basicCancel(operatorTag);
            }
            if (feedbackTag != null) {
                channel.basicCancel(feedbackTag);
            }
            channel.close();
            connection.close();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("close() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    private String getRequestID(Map<String, Object> headers) {
        if (headers == null) return UUID.randomUUID().toString();
        Object requestID = headers.get("requestId");
        if (requestID == null) return UUID.randomUUID().toString();
        return requestID.toString();
    }
}