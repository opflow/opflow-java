package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcMaster {

    private final Logger logger = LoggerFactory.getLogger(OpflowRpcMaster.class);
    final int PREFETCH_NUM = 1;
    final int CONSUMER_MAX = 1;
    
    private final Lock lock = new ReentrantLock();
    private final Condition idle = lock.newCondition();
    
    private final OpflowBroker broker;
    private final String responseName;
    
    private final boolean monitorEnabled;
    private final String monitorId;
    private final int monitorInterval;
    private final long monitorTimeout;
    
    public OpflowRpcMaster(Map<String, Object> params) throws OpflowConstructorException {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        brokerParams.put("mode", "rpc.master");
        brokerParams.put("uri", params.get("uri"));
        brokerParams.put("exchangeName", params.get("exchangeName"));
        brokerParams.put("exchangeType", "direct");
        brokerParams.put("routingKey", params.get("routingKey"));
        brokerParams.put("applicationId", params.get("applicationId"));
        broker = new OpflowBroker(brokerParams);
        responseName = (String) params.get("responseName");

        if (params.get("monitorEnabled") != null && params.get("monitorEnabled") instanceof Boolean) {
            monitorEnabled = (Boolean) params.get("monitorEnabled");
        } else {
            monitorEnabled = true;
        }
        
        monitorId = params.get("monitorId") instanceof String ? (String)params.get("monitorId"): null;
        
        if (params.get("monitorInterval") != null && params.get("monitorInterval") instanceof Integer) {
            monitorInterval = (Integer) params.get("monitorInterval");
        } else {
            monitorInterval = 2000;
        }
        
        if (params.get("monitorTimeout") != null && params.get("monitorTimeout") instanceof Long) {
            monitorTimeout = (Long) params.get("monitorTimeout");
        } else {
            monitorTimeout = 0;
        }
    }

    private final Map<String, OpflowRpcRequest> tasks = new ConcurrentHashMap<String, OpflowRpcRequest>();
    
    private OpflowBroker.ConsumerInfo responseConsumer;

    private OpflowBroker.ConsumerInfo initResponseConsumer(final boolean forked) {
        if (logger.isTraceEnabled()) logger.trace("initResponseConsumer(forked:" + forked + ")");
        return broker.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                String taskId = properties.getCorrelationId();
                if (logger.isDebugEnabled()) logger.debug("task[" + taskId + "] received data, size: " + (content != null ? content.length : -1));
                OpflowRpcRequest task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (logger.isDebugEnabled()) logger.debug("task[" + taskId + "] not found, skipped");
                } else {
                    OpflowMessage message = new OpflowMessage(content, properties.getHeaders());
                    task.push(message);
                    if (logger.isDebugEnabled()) logger.debug("task[" + taskId + "] - returned value has been pushed");
                }
                return true;
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                if (!forked) {
                    opts.put("queueName", responseName);
                    opts.put("consumerLimit", CONSUMER_MAX);
                    opts.put("forceNewChannel", Boolean.FALSE);
                }
                opts.put("binding", Boolean.FALSE);
                opts.put("prefetch", PREFETCH_NUM);
            }
        }));
    }
    
    private OpflowTask.TimeoutMonitor timeoutMonitor = null;
    
    private OpflowTask.TimeoutMonitor initTimeoutMonitor() {
        OpflowTask.TimeoutMonitor monitor = null;
        if (monitorEnabled) {
            monitor = new OpflowTask.TimeoutMonitor(tasks, monitorInterval, monitorTimeout, monitorId);
            monitor.start();
        }
        return monitor;
    }
    
    public OpflowRpcRequest request(String routineId, String content) {
        return request(routineId, content, null);
    }
    
    public OpflowRpcRequest request(String routineId, String content, Map<String, Object> opts) {
        return request(routineId, OpflowUtil.getBytes(content), opts);
    }
    
    public OpflowRpcRequest request(String routineId, byte[] content) {
        return request(routineId, content, null);
    }
    
    public OpflowRpcRequest request(String routineId, byte[] content, Map<String, Object> options) {
        Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        final boolean forked = "forked".equals((String)opts.get("mode"));
        
        if (timeoutMonitor == null) {
            timeoutMonitor = initTimeoutMonitor();
        }
        
        final OpflowBroker.ConsumerInfo consumerInfo;
        
        if (forked) {
            consumerInfo = initResponseConsumer(true);
        } else {
            if (responseConsumer == null) {
                responseConsumer = initResponseConsumer(false);
            }
            consumerInfo = responseConsumer;
        }
        
        final String taskId = UUID.randomUUID().toString();
        OpflowTask.Listener listener = new OpflowTask.Listener() {
            @Override
            public void handleEvent() {
                lock.lock();
                try {
                    tasks.remove(taskId);
                    if (tasks.isEmpty()) {
                        if (forked) {
                            broker.cancelConsumer(consumerInfo);
                        }
                        idle.signal();
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("tasks.size(): " + tasks.size());
                    }
                } finally {
                    lock.unlock();
                }
            }
        };
        
        if (routineId != null) {
            opts.put("routineId", routineId);
        }
        
        OpflowRpcRequest task = new OpflowRpcRequest(opts, listener);
        tasks.put(taskId, task);
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("requestId", task.getRequestId());
        headers.put("routineId", task.getRoutineId());
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties
                .Builder()
                .headers(headers)
                .correlationId(taskId);
        
        if (!consumerInfo.isFixedQueue()) {
            if (logger.isTraceEnabled()) {
                logger.trace("Dynamic replyTo value: " + consumerInfo.getQueueName());
            }
            builder.replyTo(consumerInfo.getQueueName());
        }
        
        broker.produce(content, builder);
        
        return task;
    }

    public void close() {
        lock.lock();
        if (logger.isTraceEnabled()) logger.trace("close() - obtain the lock");
        try {
            if (logger.isTraceEnabled()) logger.trace("close() - check tasks.isEmpty()? and await...");
            while(!tasks.isEmpty()) idle.await();
            if (logger.isTraceEnabled()) logger.trace("close() - cancel responseConsumer");
            if (responseConsumer != null) broker.cancelConsumer(responseConsumer);
            if (logger.isTraceEnabled()) logger.trace("close() - stop timeoutMonitor");
            if (timeoutMonitor != null) timeoutMonitor.stop();
            if (logger.isTraceEnabled()) logger.trace("close() - close broker/engine");
            if (broker != null) broker.close();
        } catch(InterruptedException ex) {
            if (logger.isErrorEnabled()) logger.error("close() - an exception has been thrown");
        } finally {
            lock.unlock();
            if (logger.isTraceEnabled()) logger.trace("close() - lock has been released");
        }
    }
}
