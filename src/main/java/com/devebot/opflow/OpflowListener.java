package com.devebot.opflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.Map;

public interface OpflowListener {
    public boolean processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel, String consumerTag, Map<String, Object> extras) throws IOException;
}
