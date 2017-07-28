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
public class OpflowRpcMaster {

    final Logger logger = LoggerFactory.getLogger(OpflowRpcMaster.class);

    private final OpflowEngine broker;
    private final String responseName;
    
    public OpflowRpcMaster(Map<String, Object> params) throws Exception {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        brokerParams.put("mode", "rpc.master");
        brokerParams.put("uri", params.get("uri"));
        brokerParams.put("exchangeName", params.get("exchangeName"));
        brokerParams.put("exchangeType", "direct");
        brokerParams.put("routingKey", params.get("routingKey"));
        broker = new OpflowEngine(brokerParams);
        responseName = (String) params.get("responseName");
    }

    private OpflowEngine.ConsumerInfo responseConsumer;

    public final OpflowEngine.ConsumerInfo consumeResponse() {
        if (logger.isTraceEnabled()) logger.trace("invoke consumeResponse()");
        return broker.consume(new OpflowListener() {
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
                if (logger.isDebugEnabled()) logger.debug("Message has been pushed to task[" + taskId + "]");
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void handleData(Map<String, Object> opts) {
                opts.put("queueName", responseName);
                opts.put("prefetch", 1);
                opts.put("forceNewChannel", Boolean.FALSE);
            }
        }));
    }
    
    private final Map<String, OpflowRpcResult> tasks = new HashMap<String, OpflowRpcResult>();
    
    public OpflowRpcResult request(String content, Map<String, Object> opts) {
        return request(OpflowUtil.getBytes(content), opts);
    }
    
    public OpflowRpcResult request(byte[] content, Map<String, Object> opts) {
        opts = opts != null ? opts : new HashMap<String, Object>();
        final boolean isStandalone = "standalone".equals((String)opts.get("mode"));
        
        if (responseConsumer == null) {
            responseConsumer = consumeResponse();
        }
        
        OpflowEngine.ConsumerInfo consumerInfo;
        
        if (isStandalone) {
            consumerInfo = broker.consume(new OpflowListener() {
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
                    if (logger.isDebugEnabled()) logger.debug("Message has been pushed to task[" + taskId + "]");
                }
            }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
                @Override
                public void handleData(Map<String, Object> opts) {
                    opts.put("prefetch", 1);
                }
            }));
        } else {
            consumerInfo = responseConsumer;
        }
        
        final String taskId = UUID.randomUUID().toString();
        OpflowTimeout.Listener listener = new OpflowTimeout.Listener() {
            @Override
            public void handleEvent() {
                tasks.remove(taskId);
                if (logger.isDebugEnabled()) logger.debug("tasks.size(): " + tasks.size());
            }
        };
        
        OpflowRpcResult task = new OpflowRpcResult(opts, listener);
        tasks.put(taskId, task);
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("requestId", task.getRequestId());
        headers.put("routineId", task.getRoutineId());
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties
                .Builder()
                .headers(headers)
                .correlationId(taskId);
        
        AMQP.BasicProperties props = builder.build();

        broker.produce(content, props, null);
        
        return task;
    }

    public void close() {
        if (broker != null) broker.close();
    }
}