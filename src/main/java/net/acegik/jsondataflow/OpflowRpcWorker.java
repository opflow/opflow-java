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

    private final OpflowEngine worker;
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws Exception {
        Map<String, Object> workerParams = new HashMap<String, Object>();
        workerParams.put("mode", "rpc.worker");
        workerParams.put("uri", params.get("uri"));
        workerParams.put("operator.queueName", params.get("operatorName"));
        operatorName = (String) params.get("operatorName");
        workerParams.put("feedback.queueName", params.get("responseName"));
        responseName = (String) params.get("responseName");
        worker = new OpflowEngine(workerParams);
    }

    public OpflowEngine.ConsumerInfo process(final OpflowRpcListener listener) {
        return worker.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel) throws IOException {
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, queueName);
                listener.processMessage(new OpflowMessage(content, properties.getHeaders()), response);
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.JsonListener() {
            @Override
            public void handleData(Map<String, Object> opts) {
                opts.put("queueName", operatorName);
                opts.put("replyTo", responseName);
            }
        }));
    }

    public void close() {
        if (worker != null) worker.close();
    }
}