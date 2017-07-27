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
public class OpflowRpcMaster {

    final Logger logger = LoggerFactory.getLogger(OpflowRpcMaster.class);

    private final OpflowEngine master;

    public OpflowRpcMaster(Map<String, Object> params) throws Exception {
        Map<String, Object> masterParams = new HashMap<String, Object>();
        masterParams.put("mode", "rpc.master");
        masterParams.put("uri", params.get("uri"));
        masterParams.put("exchangeName", params.get("exchangeName"));
        masterParams.put("exchangeType", "direct");
        masterParams.put("routingKey", params.get("routingKey"));
        masterParams.put("operator.queueName", params.get("operatorName"));
        masterParams.put("feedback.queueName", params.get("responseName"));
        master = new OpflowEngine(masterParams);
    }

    private boolean responseConsumed = false;

    public final void consumeResponse() {
        if (logger.isTraceEnabled()) logger.trace("invoke consumeResponse()");
        master.pullout(new OpflowListener() {
            @Override
            public void processMessage(byte[] content, AMQP.BasicProperties properties, String queueName, Channel channel) throws IOException {
                String taskId = properties.getCorrelationId();
                if (logger.isDebugEnabled()) logger.debug("received taskId: " + taskId);
                OpflowRpcResult task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (logger.isDebugEnabled()) logger.debug("task[" + taskId + "] not found. Skipped");
                    return;
                }
                OpflowMessage message = new OpflowMessage(content, properties.getHeaders());
                task.push(message);
                if (task.isCompleted(message)) {
                    tasks.remove(taskId);
                }
                if (logger.isDebugEnabled()) logger.debug("tasks.size(): " + tasks.size());
            }
        });
    }
    
    private final Map<String, OpflowRpcResult> tasks = new HashMap<String, OpflowRpcResult>();
    
    public OpflowRpcResult request(String content, Map<String, Object> opts) {
        return request(OpflowUtil.getBytes(content), opts);
    }
    
    public OpflowRpcResult request(byte[] content, Map<String, Object> opts) {
        opts = opts != null ? opts : new HashMap<String, Object>();
        
        if (!responseConsumed) {
            consumeResponse();
            responseConsumed = true;
        }
        
        if (null == opts) opts = new HashMap<String, Object>();
        OpflowRpcResult task = new OpflowRpcResult((String)opts.get("routineId"), (String)opts.get("requestId")); 
        tasks.put(task.getId(), task);
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("requestId", task.getRequestId());
        headers.put("routineId", task.getRoutineId());
        
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(task.getId())
                .headers(headers)
                .build();

        master.produce(content, props, null);
        
        return task;
    }

    public void close() {
        if (master != null) master.close();
    }
}