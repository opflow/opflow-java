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
public class OpflowRpcHandler {

    final Logger logger = LoggerFactory.getLogger(OpflowRpcHandler.class);

    private final OpflowEngine engine;

    public OpflowRpcHandler(Map<String, Object> params) throws Exception {
        engine = new OpflowEngine(params);
    }

    public void request(byte[] data, Map<String, Object> opts) {
        
    }
    
    public void process(final OpflowRpcListener listener) {
         engine.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, OpflowEngine engine) throws IOException {
                System.out.println(" [*] OpflowRpcListener: " + engine.getFeedbackQueueName());
                OpflowRpcResponse response = new OpflowRpcResponse(engine.getChannel(), properties, engine.getFeedbackQueueName());
                listener.processMessage(content, properties.getHeaders(), response);
            }
        });
    }

    public void close() {
        if (engine != null) engine.close();
    }
}