package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubHandler {
    
    final Logger logger = LoggerFactory.getLogger(OpflowPubsubHandler.class);

    private final OpflowBroker broker;
    private final String subscriberName;

    public OpflowPubsubHandler(Map<String, Object> params) throws OpflowConstructorException {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        brokerParams.put("mode", "pubsub");
        brokerParams.put("uri", params.get("uri"));
        brokerParams.put("exchangeName", params.get("exchangeName"));
        brokerParams.put("exchangeType", "direct");
        brokerParams.put("routingKey", params.get("routingKey"));
        if (params.get("otherKeys") instanceof String) {
            brokerParams.put("otherKeys", OpflowUtil.splitByComma((String)params.get("otherKeys")));
        }
        broker = new OpflowBroker(brokerParams);
        subscriberName = (String) params.get("subscriberName");
    }

    public void publish(String data, Map<String, Object> opts, String routingKey) {
        publish(OpflowUtil.getBytes(data), opts, routingKey);
    }
    
    public void publish(byte[] data, Map<String, Object> opts, String routingKey) {
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .headers(opts)
                .build();
        Map<String, Object> override = new HashMap<String, Object>();
        if (routingKey != null) {
            override.put("routingKey", routingKey);
        }
        broker.produce(data, props, override);
    }
    
    public OpflowBroker.ConsumerInfo subscribe(final OpflowPubsubListener listener) {
        return broker.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel) throws IOException {
                listener.processMessage(new OpflowMessage(content, properties.getHeaders()));
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("queueName", subscriberName);
            }
        }));
    }
}
