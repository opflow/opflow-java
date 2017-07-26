package net.acegik.jsondataflow;

import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubHandler {
    
    final Logger logger = LoggerFactory.getLogger(OpflowPubsubHandler.class);

    private final OpflowEngine engine;

    public OpflowPubsubHandler(Map<String, Object> params) throws Exception {
        engine = new OpflowEngine(params);
    }

    public void publish(byte[] data, Map<String, Object> opts) {
        
    }
    
    public void subscribe(final OpflowPubsubListener listener) {
        engine.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, OpflowEngine engine) throws IOException {
                listener.processMessage(new OpflowMessage(content, properties.getHeaders()));
            }
        });
    }
}
