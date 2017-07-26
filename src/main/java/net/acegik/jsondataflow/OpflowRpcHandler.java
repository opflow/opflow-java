package net.acegik.jsondataflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcHandler {

    final Logger logger = LoggerFactory.getLogger(OpflowRpcHandler.class);

    private final OpflowEngine master;
    private final OpflowEngine worker;

    public OpflowRpcHandler(Map<String, Object> params) throws Exception {
        Map<String, Object> masterParams = new HashMap<String, Object>();
        masterParams.put("uri", params.get("uri"));
        masterParams.put("exchangeName", params.get("exchangeName"));
        masterParams.put("exchangeType", "direct");
        masterParams.put("routingKey", params.get("routingKey"));
        masterParams.put("consumer.binding", Boolean.FALSE);
        masterParams.put("consumer.queueName", params.get("responseName"));
        master = new OpflowEngine(masterParams);
        extractResult();
        
        Map<String, Object> workerParams = new HashMap<String, Object>();
        workerParams.put("uri", params.get("uri"));
        workerParams.put("consumer.queueName", params.get("operatorName"));
        workerParams.put("feedback.queueName", params.get("responseName"));
        worker = new OpflowEngine(workerParams);
    }

    public final void extractResult() {
        master.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel) throws IOException {
                String taskId = properties.getCorrelationId();
                OpflowRpcResult task = tasks.get(taskId);
                if (taskId == null || task == null) return;
                task.push(new OpflowMessage(content, properties.getHeaders()));
            }
        });
    }
    
    private final Map<String, OpflowRpcResult> tasks = new HashMap<String, OpflowRpcResult>();
    
    public OpflowRpcResult request(String data, Map<String, Object> opts) {
        return request(OpflowUtil.getBytes(data), opts);
    }
    
    public OpflowRpcResult request(byte[] data, Map<String, Object> opts) {
        String taskId = UUID.randomUUID().toString();

        tasks.put(taskId, new OpflowRpcResult());
        
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(taskId)
                //.replyTo(this.feedback_queueName)
                .headers(opts)
                .build();

        master.produce(data, props, null);
        
        return tasks.get(taskId);
    }
    
    public void process(final OpflowRpcListener listener) {
        worker.consume(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel) throws IOException {
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, queueName);
                listener.processMessage(new OpflowMessage(content, properties.getHeaders()), response);
            }
        });
    }

    public void close() {
        if (worker != null) worker.close();
    }
}