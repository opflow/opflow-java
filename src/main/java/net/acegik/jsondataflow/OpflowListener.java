package net.acegik.jsondataflow;

import com.rabbitmq.client.AMQP;
import java.io.IOException;

public interface OpflowListener {
    public void processMessage(byte[] content, AMQP.BasicProperties properties, OpflowEngine engine) throws IOException;
}
