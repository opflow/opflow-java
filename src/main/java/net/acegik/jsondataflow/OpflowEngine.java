package net.acegik.jsondataflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowEngine {

    final Logger logger = LoggerFactory.getLogger(OpflowEngine.class);

    private static final String EXCHANGE_NAME = "sample-exchange";
    private static final String ROUTING_KEY = "sample";

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;

    private String exchangeName;
    private String routingKey;
    private String queueName;
    
    private String feedback_queueName;
    
    public Channel getChannel() {
        return channel;
    }
    
    public String getFeedbackQueueName() {
        return feedback_queueName;
    }
    
    private String getRequestID(Map<String, Object> headers, String defaultID) {
        if (headers == null) return defaultID;
        Object requestID = headers.get("requestId");
        if (requestID == null) return defaultID;
        return requestID.toString();
    }

    public OpflowEngine(Map<String, Object> params) throws Exception {
        factory = new ConnectionFactory();

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

        exchangeName = (String) params.get("exchangeName");
        if (exchangeName == null) exchangeName = EXCHANGE_NAME;

        String exchangeType = (String) params.get("exchangeType");
        if (exchangeType == null) exchangeType = "direct";

        routingKey = (String) params.get("routingKey");
        if (routingKey == null) routingKey = ROUTING_KEY;

        Map<String, Object> bindingArgs = (Map<String, Object>) params.get("bindingArgs");
        if (bindingArgs == null) bindingArgs = new HashMap<String, Object>();

        HashMap<String, Object> queueOpts = new HashMap<String, Object>();

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.basicQos(1);
        
        channel.exchangeDeclare(exchangeName, exchangeType, true);

        // declare Feedback queue
        String feedback_queueName = (String) params.get("feedback.queueName");
        if (feedback_queueName != null) {
            this.feedback_queueName = channel.queueDeclare(feedback_queueName, true, false, false, queueOpts).getQueue();
        }
        System.out.println(" [*] feedback.queueName: " + this.feedback_queueName);
        
        // declare Operator queue
        String queueName = (String) params.get("queueName");
        if (queueName != null) {
            this.queueName = channel.queueDeclare(queueName, true, false, false, queueOpts).getQueue();
        } else {
            this.queueName = channel.queueDeclare().getQueue();
        }
        System.out.println(" [*] queueName: " + this.queueName);

        // bind Operator queue to Exchange
        channel.queueBind(this.queueName, exchangeName, routingKey, bindingArgs);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }

    public void consume(final OpflowListener listener) {
        final OpflowEngine self = this;
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String requestID = getRequestID(properties.getHeaders(), "");
                String message = new String(body, "UTF-8");

                System.out.println(" [+] Message: " + message);
                
                if (logger.isDebugEnabled()) logger.debug("Request[" + requestID + "] consumes new message");
                if (logger.isDebugEnabled()) logger.debug("Request[" + requestID + "] fire an event");

                listener.processMessage(body, properties, self);

                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        
        try {
            String consumerTag = channel.basicConsume(this.queueName, false, consumer);
            System.out.println(" [*] Invoke basicConsume(" + this.queueName + ") -> " + consumerTag);
            System.out.println(" [*] Send test to exchange[" + this.exchangeName + "] with routingKey " + this.routingKey);
            //channel.basicPublish(this.exchangeName, this.routingKey, null, "test".getBytes());
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("run() has been failed, exception: " + exception.getMessage());
             System.out.println("run() has been failed, exception: " + exception.getMessage());
            throw new OpflowGeneralException(exception);
        }
    }

    public void close() {
        try {
            System.out.println(" [*] Close....");
            channel.close();
            connection.close();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("close() has been failed, exception: " + exception.getMessage());
            throw new OpflowGeneralException(exception);
        }
    }
}