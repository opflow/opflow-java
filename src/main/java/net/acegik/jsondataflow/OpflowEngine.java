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

/**
 *
 * @author drupalex
 */
public class OpflowEngine {

    final Logger logger = LoggerFactory.getLogger(OpflowEngine.class);

    private static final String ROUTING_KEY = "sample";

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;
    private String consumerTag;

    private String exchangeName;
    private String routingKey;

    private String consumer_queueName;
    private String feedback_queueName;
    
    public Channel getChannel() {
        return channel;
    }
    
    public String getFeedbackQueueName() {
        return feedback_queueName;
    }
    
    private String getRequestID(Map<String, Object> headers) {
        if (headers == null) return UUID.randomUUID().toString();
        Object requestID = headers.get("requestId");
        if (requestID == null) return UUID.randomUUID().toString();
        return requestID.toString();
    }

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

        String exchangeName = (String) params.get("exchangeName");
        if (exchangeName != null) this.exchangeName = exchangeName;

        String exchangeType = (String) params.get("exchangeType");
        if (exchangeType == null) exchangeType = "direct";

        routingKey = (String) params.get("routingKey");
        if (routingKey == null) routingKey = ROUTING_KEY;

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.basicQos(1);
        
        if (this.exchangeName != null) {
            channel.exchangeDeclare(this.exchangeName, exchangeType, true);
        }

        HashMap<String, Object> queueOpts = new HashMap<String, Object>();
        
        // declare Feedback queue
        String queueName = (String) params.get("feedback.queueName");
        if (queueName != null) {
            this.feedback_queueName = channel.queueDeclare(queueName, true, false, false, queueOpts).getQueue();
        }
        if (logger.isTraceEnabled()) logger.trace("feedback_queueName: " + this.feedback_queueName);
        
        // declare Operator queue
        queueName = (String) params.get("consumer.queueName");
        if (queueName != null) {
            this.consumer_queueName = channel.queueDeclare(queueName, true, false, false, queueOpts).getQueue();
        } else {
            this.consumer_queueName = channel.queueDeclare().getQueue();
        }
        if (logger.isTraceEnabled()) logger.trace("consumer_queueName: " + this.consumer_queueName);

        // bind Operator queue to Exchange
        Boolean binding = (Boolean) params.get("consumer.binding");
        if (!Boolean.FALSE.equals(binding) && 
                this.exchangeName != null && 
                this.consumer_queueName != null && 
                this.routingKey != null) {
            Map<String, Object> bindingArgs = (Map<String, Object>) params.get("bindingArgs");
            if (bindingArgs == null) bindingArgs = new HashMap<String, Object>();
            channel.queueBind(this.consumer_queueName, this.exchangeName, routingKey, bindingArgs);
            if (logger.isTraceEnabled()) {
                logger.trace(MessageFormat.format("Exchange[{0}] binded to Queue[{1}] with routingKey[{2}]", new Object[] {
                    this.exchangeName, this.consumer_queueName, this.routingKey
                }));
            }
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
            throw new OpflowGeneralException(exception);
        }
    }
    
    public void consume(final OpflowListener listener) {
        final Channel _channel = this.channel;
        final String _queueName = this.feedback_queueName;
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String requestID = getRequestID(properties.getHeaders());
                
                if (logger.isInfoEnabled()) {
                    logger.info(MessageFormat.format("Request[{0}] / DeliveryTag[{1}] handled in consumerTag[{2}]", new Object[] {
                        requestID, envelope.getDeliveryTag(), consumerTag
                    }));
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
                listener.processMessage(body, properties, _queueName, _channel);

                if (logger.isInfoEnabled()) {
                    logger.info(MessageFormat.format("Request[{0}] invoke Ack({1}, false))", new Object[] {
                        requestID, envelope.getDeliveryTag()
                    }));
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        
        try {
            consumerTag = channel.basicConsume(this.consumer_queueName, false, consumer);
            if (logger.isInfoEnabled()) {
                logger.info("[*] Consume queue[" + this.consumer_queueName + "] -> consumerTag: " + consumerTag);
                logger.info("[*] Waiting for messages. To exit press CTRL+C");
            }
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() has been failed, exception: " + exception.getMessage());
            throw new OpflowGeneralException(exception);
        }
    }

    public void close() {
        try {
            if (logger.isInfoEnabled()) logger.info("[*] Cancel consumer; close channel, connection.");
            if (consumerTag != null) {
                channel.basicCancel(consumerTag);
            }
            channel.close();
            connection.close();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("close() has been failed, exception: " + exception.getMessage());
            throw new OpflowGeneralException(exception);
        }
    }
}