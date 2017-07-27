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
    private String routingKey;

    private Consumer operator;
    private String operatorTag;
    private String operatorName;
    
    private Consumer feedback;
    private String feedbackTag;
    private String feedbackName;

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
        
        connection = factory.newConnection();
        channel = connection.createChannel();

        String mode = (String) params.get("mode");
        if ("rpc.master".equals(mode)) {
            channel.basicQos(1);
        }

        String exchangeName = (String) params.get("exchangeName");
        if (exchangeName != null) this.exchangeName = exchangeName;

        String exchangeType = (String) params.get("exchangeType");
        if (exchangeType == null) exchangeType = "direct";

        if (this.exchangeName != null) {
            channel.exchangeDeclare(this.exchangeName, exchangeType, true);
        }
        
        String queueName;
        
        // declare Operator queue
        queueName = (String) params.get("operator.queueName");
        if (queueName != null) {
            this.operatorName = channel.queueDeclare(queueName, true, false, false, null).getQueue();
        } else {
            this.operatorName = channel.queueDeclare().getQueue();
        }
        if (logger.isTraceEnabled()) logger.trace("operatorName: " + this.operatorName);

        // declare Feedback queue
        queueName = (String) params.get("feedback.queueName");
        if (queueName != null) {
            this.feedbackName = channel.queueDeclare(queueName, true, false, false, null).getQueue();
        }
        if (logger.isTraceEnabled()) logger.trace("feedbackName: " + this.feedbackName);

        // bind Operator queue to Exchange
        routingKey = (String) params.get("routingKey");
        Boolean binding = (Boolean) params.get("operator.binding");
        if (!Boolean.FALSE.equals(binding) && this.routingKey != null && 
                this.exchangeName != null && this.operatorName != null) {
            Map<String, Object> bindingArgs = (Map<String, Object>) params.get("bindingArgs");
            if (bindingArgs == null) bindingArgs = new HashMap<String, Object>();
            channel.queueBind(this.operatorName, this.exchangeName, this.routingKey, bindingArgs);
            if (logger.isTraceEnabled()) {
                logger.trace(MessageFormat.format("Exchange[{0}] binded to Queue[{1}] with routingKey[{2}]", new Object[] {
                    this.exchangeName, this.operatorName, this.routingKey
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
            throw new OpflowOperationException(exception);
        }
    }
    
    public void consume(final OpflowListener listener) {
        final Channel _channel = this.channel;
        final String _queueName = this.feedbackName;
        operator = new DefaultConsumer(channel) {
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
                listener.processMessage(body, properties, _queueName, _channel);

                if (logger.isTraceEnabled()) {
                    logger.trace(MessageFormat.format("Request[{0}] invoke Ack({1}, false)) / ConsumerTag[{2}]", new Object[] {
                        requestID, envelope.getDeliveryTag(), consumerTag
                    }));
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
                
                if (logger.isInfoEnabled()) {
                    logger.info("Request[" + requestID + "] has finished successfully");
                }
            }
        };
        
        try {
            operatorTag = channel.basicConsume(this.operatorName, false, operator);
            if (logger.isInfoEnabled()) {
                logger.info("[*] Consume queue[" + this.operatorName + "] -> consumerTag: " + operatorTag);
            }
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    public void pullout(final OpflowListener listener) {
        feedback = new DefaultConsumer(channel) {
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
                
                listener.processMessage(body, properties, null, null);

                channel.basicAck(envelope.getDeliveryTag(), false);
                
                if (logger.isInfoEnabled()) {
                    logger.info("Request[" + requestID + "] has finished successfully");
                }
            }
        };
        
        try {
            feedbackTag = channel.basicConsume(this.feedbackName, false, feedback);
            if (logger.isInfoEnabled()) {
                logger.info("[*] Consume queue[" + this.feedbackName + "] -> consumerTag: " + feedbackTag);
            }
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() has been failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
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
}