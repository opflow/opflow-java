package net.acegik.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import com.rabbitmq.client.*;

public class JsonObjectFlow implements Runnable {
    
    private static final String EXCHANGE_NAME = "sample-exchange";
    private static final String ROUTING_KEY = "sample";

    ConnectionFactory factory;
    Connection connection;
    Channel channel;
    Consumer consumer;
    String queueName;

    private List<FlowChangeListener> listeners = new ArrayList<FlowChangeListener>();

    public void addListener(FlowChangeListener listener) {
        listeners.add(listener);
    }

    public void removeListener(FlowChangeListener listener) {
        listeners.remove(listener);
    }

    private void fireListener(FlowChangeEvent event) {
        for(FlowChangeListener listener: listeners) {
            listener.objectReceived(event);
        }
    }

    public JsonObjectFlow(Map<String, Object> params) throws Exception {
        String host = (String) params.get("host");
        if (host == null) host = "localhost";

        String virtualHost = (String) params.get("virtualHost");
        String username = (String) params.get("username");
        String password = (String) params.get("password");

        factory = new ConnectionFactory();
        factory.setHost(host);

        if (virtualHost != null) {
            factory.setVirtualHost(virtualHost);
        }
        if (username != null) {
            factory.setUsername(username);
        }
        if (password != null) {
            factory.setPassword(password);
        }

        String exchangeName = (String) params.get("exchangeName");
        if (exchangeName == null) exchangeName = EXCHANGE_NAME;

        String exchangeType = (String) params.get("exchangeType");
        if (exchangeType == null) exchangeType = "direct";

        String routingKey = (String) params.get("routingKey");
        if (routingKey == null) routingKey = ROUTING_KEY;

        String queueName = (String) params.get("queueName");
        if (queueName == null) queueName = channel.queueDeclare().getQueue();
        this.queueName = queueName;

        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, exchangeType, true);
        channel.queueBind(this.queueName, exchangeName, routingKey);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                FlowChangeEvent event = new FlowChangeEvent("received", message);
                fireListener(event);
            }
        };
    }

    public void run() {
        try {
            channel.basicConsume(this.queueName, true, consumer);
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    public void close() throws Exception {
        channel.close();
        connection.close();
    }
}
