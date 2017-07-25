package net.acegik.jsondataflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowChangeFeedback {

    private final Channel channel;
    private final AMQP.BasicProperties properties;
    private final String queueName;
    
    public OpflowChangeFeedback(Channel channel, AMQP.BasicProperties properties, String queueName) {
        this.channel = channel;
        this.properties = properties;
        this.queueName = queueName;
    }
    
    private void basicPublish(String data, AMQP.BasicProperties replyProps) {
        try {
            channel.basicPublish("", queueName, replyProps, data.getBytes("UTF-8"));
        } catch (IOException exception) {
            throw new OpflowGeneralException(exception);
        }
    }
    
    public void emitStarted() {
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("status", "started");
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
            .Builder()
            .correlationId(properties.getCorrelationId())
            .headers(headers)
            .build();
        basicPublish("{}", replyProps);
    }
    
    public void emitProgress(int completed, int total, String extra) {
        
    }
    
    public void emitFailed() {
        
    }
    
    public void emitCompleted(String result) {
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("status", "completed");
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
            .Builder()
            .correlationId(properties.getCorrelationId())
            .headers(headers)
            .build();
        basicPublish(result, replyProps);
    }
}
