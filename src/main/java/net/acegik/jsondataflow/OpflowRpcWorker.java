package net.acegik.jsondataflow;

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
public class OpflowRpcWorker {

    final Logger logger = LoggerFactory.getLogger(OpflowRpcWorker.class);

    private final OpflowEngine broker;
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws Exception {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        brokerParams.put("mode", "rpc.worker");
        brokerParams.put("uri", params.get("uri"));
        brokerParams.put("exchangeName", params.get("exchangeName"));
        brokerParams.put("exchangeType", "direct");
        brokerParams.put("routingKey", params.get("routingKey"));
        broker = new OpflowEngine(brokerParams);
        operatorName = (String) params.get("operatorName");
        responseName = (String) params.get("responseName");
    }

    public OpflowEngine.ConsumerInfo process(final OpflowRpcListener listener) {
        return broker.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel) throws IOException {
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, queueName);
                listener.processMessage(new OpflowMessage(content, properties.getHeaders()), response);
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void handleData(Map<String, Object> opts) {
                opts.put("queueName", operatorName);
                opts.put("replyTo", responseName);
                opts.put("binding", Boolean.TRUE);
            }
        }));
    }

    public void close() {
        if (broker != null) broker.close();
    }
}