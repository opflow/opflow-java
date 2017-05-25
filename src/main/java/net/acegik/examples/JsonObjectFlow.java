package net.acegik.examples;

import java.io.IOException;
import java.util.Random;
import com.rabbitmq.client.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonObjectFlow extends Thread {
    
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
	            JsonParser jsonParser = new JsonParser();
	            JsonObject jsonObject = (JsonObject)jsonParser.parse(message);

	            System.out.println(" [x] Received '" + jsonObject.toString() + "'");
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
