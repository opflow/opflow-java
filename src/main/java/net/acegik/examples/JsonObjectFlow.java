package net.acegik.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.rabbitmq.client.*;

public class JsonObjectFlow implements Runnable {
    
    private static final String EXCHANGE_NAME = "sample-exchange";
    private static final String QUEUE_NAME = "sample-queue";

    ConnectionFactory factory;
    String host = "192.168.56.56";
    String virtualHost = "/";
    String username = "master";
    String password = "zaq123edcx";

    Connection connection;
    Channel channel;
    Consumer consumer;

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

    public JsonObjectFlow() throws Exception {
    	factory = new ConnectionFactory();
    	factory.setHost(host);
	    factory.setVirtualHost(virtualHost);
	    factory.setUsername(username);
	    factory.setPassword(password);

	    connection = factory.newConnection();
	    channel = connection.createChannel();

	    channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
	    //String queueName = QUEUE_NAME; // channel.queueDeclare().getQueue();
	    channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "sample");

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
    		channel.basicConsume(QUEUE_NAME, true, consumer);
    	} catch (Exception exception) {
    		throw new RuntimeException(exception);
    	}
    }
}
