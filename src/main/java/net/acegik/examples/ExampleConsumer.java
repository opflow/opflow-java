package net.acegik.examples;

import com.rabbitmq.client.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;

public class ExampleConsumer {
    private static final String EXCHANGE_NAME = "sample-exchange";
    private static final String QUEUE_NAME = "sample-queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        String host = "192.168.56.56";
        String virtualHost = "/";
        String username = "master";
        String password = "zaq123edcx";


        factory.setHost(host);
        factory.setVirtualHost(virtualHost);
        factory.setUsername(username);
        factory.setPassword(password);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct", true);
        String queueName = QUEUE_NAME; // channel.queueDeclare().getQueue();
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "sample");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                    AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                JsonParser jsonParser = new JsonParser();
                JsonObject jsonObject = (JsonObject)jsonParser.parse(message);

                System.out.println(" [x] Received '" + jsonObject.toString() + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
