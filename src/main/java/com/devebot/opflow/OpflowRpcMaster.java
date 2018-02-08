package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    private final OpflowLogTracer logTracer;
    
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
        params = OpflowUtil.ensureNotNull(params);
        
        final String rpcMasterId = OpflowUtil.getOptionField(params, "rpcMasterId", true);
        logTracer = OpflowLogTracer.ROOT.branch("rpcMasterId", rpcMasterId);
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("RpcMaster[${rpcMasterId}].new()")
                .stringify());
        
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
        
        monitorId = params.get("monitorId") instanceof String ? (String)params.get("monitorId") : rpcMasterId;
        
        if (params.get("monitorInterval") != null && params.get("monitorInterval") instanceof Integer) {
            monitorInterval = (Integer) params.get("monitorInterval");
        } else {
            monitorInterval = 14000; // can run 2-3 times in 30s
        }
        
        if (params.get("monitorTimeout") != null && params.get("monitorTimeout") instanceof Long) {
            monitorTimeout = (Long) params.get("monitorTimeout");
        } else {
            monitorTimeout = 0;
        }
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .put("responseName", responseName)
                .put("monitorId", monitorId)
                .put("monitorEnabled", monitorEnabled)
                .put("monitorInterval", monitorInterval)
                .put("monitorTimeout", monitorTimeout)
                .tags("RpcMaster.new() parameters")
                .text("RpcMaster[${rpcMasterId}].new() parameters")
                .stringify());
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("RpcMaster[${rpcMasterId}].new() end!")
                .stringify());
    }

    private final Map<String, OpflowRpcRequest> tasks = new ConcurrentHashMap<String, OpflowRpcRequest>();
    
    private OpflowEngine.ConsumerInfo responseConsumer;

    private OpflowEngine.ConsumerInfo initResponseConsumer(final boolean forked) {
        final String _consumerId = OpflowUtil.getLogID();
        final OpflowLogTracer logSession = logTracer.branch("consumerId", _consumerId);
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logSession
                .put("forked", forked)
                .text("initResponseConsumer() is invoked")
                .stringify());
        return engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                String taskId = properties.getCorrelationId();
                Map<String, Object> headers = properties.getHeaders();

                String requestId = OpflowUtil.getRequestId(headers, true);
                OpflowLogTracer logResult = null;
                if (OpflowLogTracer.has(LOG, "info")) logResult = logSession.branch("requestId", requestId);

                if (OpflowLogTracer.has(LOG, "info") && logResult != null) LOG.info(logResult
                        .put("correlationId", taskId)
                        .text("initResponseConsumer() - receives a result")
                        .stringify());

                if (OpflowLogTracer.has(LOG, "debug") && logResult != null) LOG.debug(logResult
                        .put("bodyLength", (content != null ? content.length : -1))
                        .text("initResponseConsumer() - result body length")
                        .stringify());

                OpflowRpcRequest task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (OpflowLogTracer.has(LOG, "debug") && logResult != null) LOG.debug(logResult
                        .put("correlationId", taskId)
                        .text("initResponseConsumer() - task not found, skipped")
                        .stringify());
                } else {
                    if (OpflowLogTracer.has(LOG, "debug") && logResult != null) LOG.debug(logResult
                        .put("correlationId", taskId)
                        .text("initResponseConsumer() - push Message object to Task")
                        .stringify());
                    OpflowMessage message = new OpflowMessage(content, properties.getHeaders());
                    task.push(message);
                    if (OpflowLogTracer.has(LOG, "debug") && logResult != null) LOG.debug(logResult
                        .put("correlationId", taskId)
                        .text("initResponseConsumer() - returned value has been pushed")
                        .stringify());
                }
                return true;
            }
        }, OpflowUtil.buildMap(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("consumerId", _consumerId);
                if (!forked) {
                    opts.put("queueName", responseName);
                    opts.put("consumerLimit", CONSUMER_MAX);
                    opts.put("forceNewChannel", Boolean.FALSE);
                }
                opts.put("binding", Boolean.FALSE);
                opts.put("prefetch", PREFETCH_NUM);
            }
        }).toMap());
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
    
    public OpflowRpcRequest request(String routineId, String body) {
        return request(routineId, body, null);
    }
    
    public OpflowRpcRequest request(String routineId, String body, Map<String, Object> options) {
        return request(routineId, OpflowUtil.getBytes(body), options);
    }
    
    public OpflowRpcRequest request(String routineId, byte[] body) {
        return request(routineId, body, null);
    }
    
    public OpflowRpcRequest request(String routineId, byte[] body, Map<String, Object> options) {
        options = OpflowUtil.ensureNotNull(options);
        
        Object requestId = options.get("requestId");
        if (requestId == null) {
            options.put("requestId", requestId = OpflowUtil.getLogID());
        }
        
        final OpflowLogTracer logRequest = logTracer.branch("requestId", requestId);
        
        if (routineId != null) {
            options.put("routineId", routineId);
        }
        
        if (timeoutMonitor == null) {
            timeoutMonitor = initTimeoutMonitor();
        }
        
        final boolean forked = "forked".equals((String)options.get("mode"));
        final OpflowEngine.ConsumerInfo consumerInfo;
        if (forked) {
            consumerInfo = initResponseConsumer(true);
        } else {
            if (responseConsumer == null) {
                responseConsumer = initResponseConsumer(false);
            }
            consumerInfo = responseConsumer;
        }
        
        final String taskId = OpflowUtil.getLogID();
        OpflowTask.Listener listener = new OpflowTask.Listener() {
            private OpflowLogTracer logTask = null;
            @Override
            public void handleEvent() {
                lock.lock();
                if (OpflowLogTracer.has(LOG, "debug") && logRequest != null) logTask = logRequest.copy();
                try {
                    tasks.remove(taskId);
                    if (tasks.isEmpty()) {
                        if (forked) {
                            engine.cancelConsumer(consumerInfo);
                        }
                        idle.signal();
                    }
                    if (logTask != null && OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTask
                            .put("taskListSize", tasks.size())
                            .text("Request[${requestId}] - RpcMaster[${rpcMasterId}] - Check tasks size after removing a task")
                            .stringify());
                } finally {
                    lock.unlock();
                }
            }
        };
        
        OpflowRpcRequest task = new OpflowRpcRequest(options, listener);
        tasks.put(taskId, task);
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("requestId", task.getRequestId());
        headers.put("routineId", task.getRoutineId());
        
        Boolean progressEnabled = null;
        if (options.get("progressEnabled") instanceof Boolean) {
            headers.put("progressEnabled", progressEnabled = (Boolean) options.get("progressEnabled"));
        }
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .correlationId(taskId);
        
        if (!consumerInfo.isFixedQueue()) {
            if (OpflowLogTracer.has(LOG, "trace") && logRequest != null) LOG.trace(logRequest
                    .put("replyTo", consumerInfo.getQueueName())
                    .text("Request[${requestId}] - RpcMaster[${rpcMasterId}] - Use dynamic replyTo: ${replyTo}")
                    .stringify());
            builder.replyTo(consumerInfo.getQueueName());
        }
        
        engine.produce(body, headers, builder);
        
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
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - obtain the lock")
                .stringify());
        try {
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - check tasks.isEmpty()? and await...")
                .stringify());
            while(!tasks.isEmpty()) idle.await();
            
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - cancel responseConsumer")
                .stringify());
            if (responseConsumer != null) engine.cancelConsumer(responseConsumer);
            
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - stop timeoutMonitor")
                .stringify());
            if (timeoutMonitor != null) timeoutMonitor.stop();
            
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - close broker/engine")
                .stringify());
            if (engine != null) engine.close();
        } catch(InterruptedException ex) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - an interruption has been raised")
                .stringify());
        } finally {
            lock.unlock();
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - lock has been released")
                .stringify());
        }
    }
    
    public OpflowExecutor getExecutor() {
        return executor;
    }
    
    public String getResponseName() {
        return responseName;
    }
}
