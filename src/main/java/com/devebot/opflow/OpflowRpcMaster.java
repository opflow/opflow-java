package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
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
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcMaster.class);
    private final OpflowLogTracer logTracer = new OpflowLogTracer();
    
    private final int PREFETCH_NUM = 1;
    private final int CONSUMER_MAX = 1;
    
    private final Lock lock = new ReentrantLock();
    private final Condition idle = lock.newCondition();
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final String responseName;
    
    private final boolean monitorEnabled;
    private final String monitorId;
    private final int monitorInterval;
    private final long monitorTimeout;
    
    public OpflowRpcMaster(Map<String, Object> params) throws OpflowBootstrapException {
        final String rpcMasterId = OpflowUtil.getOptionField(params, "rpcMasterId", true);
        logTracer.put("rpcMasterId", rpcMasterId);
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "RpcMaster.new()")
                .toString());
        
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put("engineId", rpcMasterId);
        brokerParams.put("mode", "rpc_master");
        brokerParams.put("exchangeType", "direct");
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        responseName = (String) params.get("responseName");
        if (responseName != null) {
            executor.assertQueue(responseName);
        }
        
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
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer.copy()
                .put("responseName", responseName)
                .put("message", "RpcMaster.new() parameters")
                .toString());
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "RpcMaster.new() end!")
                .toString());
    }

    private final Map<String, OpflowRpcRequest> tasks = new ConcurrentHashMap<String, OpflowRpcRequest>();
    
    private OpflowEngine.ConsumerInfo responseConsumer;

    private OpflowEngine.ConsumerInfo initResponseConsumer(final boolean forked) {
        if (LOG.isTraceEnabled()) LOG.trace("initResponseConsumer(forked:" + forked + ")");
        return engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                String taskId = properties.getCorrelationId();
                if (LOG.isDebugEnabled()) LOG.debug("task[" + taskId + "] received data, size: " + (content != null ? content.length : -1));
                OpflowRpcRequest task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (LOG.isDebugEnabled()) LOG.debug("task[" + taskId + "] not found, skipped");
                } else {
                    OpflowMessage message = new OpflowMessage(content, properties.getHeaders());
                    task.push(message);
                    if (LOG.isDebugEnabled()) LOG.debug("task[" + taskId + "] - returned value has been pushed");
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
        
        final OpflowEngine.ConsumerInfo consumerInfo;
        
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
                            engine.cancelConsumer(consumerInfo);
                        }
                        idle.signal();
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("tasks.size(): " + tasks.size());
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
        
        Boolean progressEnabled = opts.get("progressEnabled") instanceof Boolean ? (Boolean) opts.get("progressEnabled") : null;
        if (progressEnabled != null) headers.put("progressEnabled", progressEnabled);
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties
                .Builder()
                .headers(headers)
                .correlationId(taskId);
        
        if (!consumerInfo.isFixedQueue()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Dynamic replyTo value: " + consumerInfo.getQueueName());
            }
            builder.replyTo(consumerInfo.getQueueName());
        }
        
        engine.produce(content, builder);
        
        return task;
    }

    public class State extends OpflowEngine.State {
        public State(OpflowEngine.State superState) {
            super(superState);
        }
    }
    
    public State check() {
        State state = new State(engine.check());
        return state;
    }
    
    public void close() {
        lock.lock();
        if (LOG.isTraceEnabled()) LOG.trace("close() - obtain the lock");
        try {
            if (LOG.isTraceEnabled()) LOG.trace("close() - check tasks.isEmpty()? and await...");
            while(!tasks.isEmpty()) idle.await();
            if (LOG.isTraceEnabled()) LOG.trace("close() - cancel responseConsumer");
            if (responseConsumer != null) engine.cancelConsumer(responseConsumer);
            if (LOG.isTraceEnabled()) LOG.trace("close() - stop timeoutMonitor");
            if (timeoutMonitor != null) timeoutMonitor.stop();
            if (LOG.isTraceEnabled()) LOG.trace("close() - close broker/engine");
            if (engine != null) engine.close();
        } catch(InterruptedException ex) {
            if (LOG.isErrorEnabled()) LOG.error("close() - an exception has been thrown");
        } finally {
            lock.unlock();
            if (LOG.isTraceEnabled()) LOG.trace("close() - lock has been released");
        }
    }
    
    public OpflowExecutor getExecutor() {
        return executor;
    }
    
    public String getResponseName() {
        return responseName;
    }
}
