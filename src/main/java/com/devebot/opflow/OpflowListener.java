package com.devebot.opflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;

public interface OpflowListener {
    public boolean processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel, String workerTag) throws IOException;
}
