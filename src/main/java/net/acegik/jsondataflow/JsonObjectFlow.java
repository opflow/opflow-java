package net.acegik.jsondataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonObjectFlow implements Runnable {

    final Logger logger = LoggerFactory.getLogger(JsonObjectFlow.class);

    private static final String EXCHANGE_NAME = "sample-exchange";
    private static final String ROUTING_KEY = "sample";

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;
    private String queueName;

    private List<FlowChangeListener> listeners = new ArrayList<FlowChangeListener>();

    public void addListener(FlowChangeListener listener) {
        listeners.add(listener);
    }

    public void removeListener(FlowChangeListener listener) {
        listeners.remove(listener);
    }

    private void fireEvent(FlowChangeEvent event) {
        for(FlowChangeListener listener: listeners) {
            listener.objectReceived(event);
        }
    }

    private String getRequestID(Map<String, Object> headers, String defaultID) {
        if (headers == null) return defaultID;
        Object requestID = headers.get("X-Request-ID");
        if (requestID == null) return defaultID;
        return requestID.toString();
    }

    public JsonObjectFlow(Map<String, Object> params) throws Exception {
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

        String exchangeName = (String) params.get("exchangeName");
        if (exchangeName == null) exchangeName = EXCHANGE_NAME;

        String exchangeType = (String) params.get("exchangeType");
        if (exchangeType == null) exchangeType = "direct";

        String routingKey = (String) params.get("routingKey");
        if (routingKey == null) routingKey = ROUTING_KEY;

        Map<String, Object> bindingArgs = (Map<String, Object>) params.get("bindingArgs");
        if (bindingArgs == null) bindingArgs = new HashMap<String, Object>();

        HashMap<String, Object> queueOpts = new HashMap<String, Object>();

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, exchangeType, true);

        String queueName = (String) params.get("queueName");
        if (queueName != null) {
            this.queueName = channel.queueDeclare(queueName, true, false, false, queueOpts).getQueue();
        } else {
            this.queueName = channel.queueDeclare().getQueue();
        }
        System.out.println(" [*] queueName: " + this.queueName);

        channel.queueBind(this.queueName, exchangeName, routingKey, bindingArgs);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String requestID = getRequestID(properties.getHeaders(), "");
                if (logger.isDebugEnabled()) logger.debug("RequestID[" + requestID + "] consumes new message");
                String message = new String(body, "UTF-8");
                FlowChangeEvent event = new FlowChangeEvent("received", message);
                if (logger.isDebugEnabled()) logger.debug("RequestID[" + requestID + "] fire an event");
                fireEvent(event);
            }
        };
    }

    @Override
    public void run() {
        try {
            System.out.println(" [*] Invoke basicConsume()");
            channel.basicConsume(this.queueName, true, consumer);
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("run() has been failed, exception: " + exception.getMessage());
            throw new GeneralException(exception);
        }
    }

    public void close() {
        try {
            System.out.println(" [*] Close....");
            channel.close();
            connection.close();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("close() has been failed, exception: " + exception.getMessage());
            throw new GeneralException(exception);
        }
    }
}