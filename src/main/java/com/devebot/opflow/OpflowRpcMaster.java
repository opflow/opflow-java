package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcMaster implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcMaster.class);
    
    private final long DELAY_TIMEOUT = 1000;
    private final int PREFETCH_NUM = 1;
    private final int CONSUMER_MAX = 1;
    
    private final String rpcMasterId;
    private final OpflowLogTracer logTracer;
    private final OpflowExporter exporter;
    
    private final ExecutorService threadExecutor = Executors.newSingleThreadExecutor();
    private final Lock lock = new ReentrantLock();
    private final Condition idle = lock.newCondition();
    private final ReentrantReadWriteLock pushLock = new ReentrantReadWriteLock();
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final long expiration;
    private final String responseName;
    private final Boolean responseDurable;
    private final Boolean responseExclusive;
    private final Boolean responseAutoDelete;
    
    private final boolean monitorEnabled;
    private final String monitorId;
    private final int monitorInterval;
    private final long monitorTimeout;
    
    public OpflowRpcMaster(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowUtil.ensureNotNull(params);
        
        rpcMasterId = OpflowUtil.getOptionField(params, "rpcMasterId", true);
        logTracer = OpflowLogTracer.ROOT.branch("rpcMasterId", rpcMasterId);
        
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("RpcMaster[${rpcMasterId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put("engineId", rpcMasterId);
        brokerParams.put("mode", "rpc_master");
        brokerParams.put("exchangeType", "direct");
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        if (params.get("expiration") != null && params.get("expiration") instanceof Long) {
            expiration = (Long) params.get("expiration");
        } else {
            expiration = 0;
        }
        
        String responseQueuePattern = null;
        if (params.get("responseQueueSuffix") instanceof String) {
            responseQueuePattern = (String) params.get("responseQueueSuffix");
        }
        
        String responseQueueSuffix = null;
        if (responseQueuePattern != null && responseQueuePattern.length() > 0) {
            if (responseQueuePattern.equals("~")) {
                responseQueueSuffix = OpflowUtil.getUUID();
            } else {
                responseQueueSuffix = responseQueuePattern;
            }
        }
        
        String _responseName = (String) params.get("responseName");
        if (_responseName != null) {
            responseName = responseQueueSuffix != null ? _responseName + '_' + responseQueueSuffix : _responseName;
        } else {
            responseName = null;
        }
        
        if (params.get("responseDurable") != null && params.get("responseDurable") instanceof Boolean) {
            responseDurable = (Boolean) params.get("responseDurable");
        } else {
            responseDurable = responseQueueSuffix != null ? false : null;
        }
        
        if (params.get("responseExclusive") != null && params.get("responseExclusive") instanceof Boolean) {
            responseExclusive = (Boolean) params.get("responseExclusive");
        } else {
            responseExclusive = responseQueueSuffix != null ? true : null;
        }
        
        if (params.get("responseAutoDelete") != null && params.get("responseAutoDelete") instanceof Boolean) {
            responseAutoDelete = (Boolean) params.get("responseAutoDelete");
        } else {
            responseAutoDelete = responseQueueSuffix != null ? true : null;
        }
        
        if (responseName != null) {
            executor.assertQueue(responseName, responseDurable, responseExclusive, responseAutoDelete);
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

        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .put("responseName", responseName)
                .put("responseDurable", responseDurable)
                .put("responseExclusive", responseExclusive)
                .put("responseAutoDelete", responseAutoDelete)
                .put("monitorId", monitorId)
                .put("monitorEnabled", monitorEnabled)
                .put("monitorInterval", monitorInterval)
                .put("monitorTimeout", monitorTimeout)
                .tags("RpcMaster.new() parameters")
                .text("RpcMaster[${rpcMasterId}].new() parameters")
                .stringify());

        exporter = OpflowExporter.getInstance();
        
        exporter.changeComponentInstance("rpc_master", rpcMasterId, OpflowExporter.GaugeAction.INC);
        
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("RpcMaster[${rpcMasterId}].new() end!")
                .stringify());
    }

    private final Map<String, OpflowRpcRequest> tasks = new ConcurrentHashMap<>();
    
    private OpflowEngine.ConsumerInfo callbackConsumer;

    private OpflowEngine.ConsumerInfo initCallbackConsumer(final boolean forked) {
        final String _consumerId = OpflowUtil.getLogID();
        final OpflowLogTracer logSession = logTracer.branch("consumerId", _consumerId);
        if (logSession.ready(LOG, "info")) LOG.info(logSession
                .put("forked", forked)
                .text("initCallbackConsumer() is invoked with [forked]: ${forked}")
                .stringify());
        return engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(
                    byte[] content,
                    AMQP.BasicProperties properties,
                    String queueName,
                    Channel channel,
                    String workerTag
            ) throws IOException {
                String taskId = properties.getCorrelationId();
                Map<String, Object> headers = properties.getHeaders();

                String requestId = OpflowUtil.getRequestId(headers, true);
                OpflowLogTracer logResult = null;
                if (logSession.ready(LOG, "info")) {
                    logResult = logSession.branch("requestId", requestId, new OpflowLogTracer.OmitPingLogs(headers));
                }

                if (logResult != null && logResult.ready(LOG, "info")) LOG.info(logResult
                        .put("correlationId", taskId)
                        .text("initCallbackConsumer() - task[${correlationId}] receives a result")
                        .stringify());

                if (logResult != null && logResult.ready(LOG, "debug")) LOG.debug(logResult
                        .put("bodyLength", (content != null ? content.length : -1))
                        .text("initCallbackConsumer() - result body length")
                        .stringify());

                OpflowRpcRequest task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (logResult != null && logResult.ready(LOG, "debug")) LOG.debug(logResult
                        .put("correlationId", taskId)
                        .text("initCallbackConsumer() - task[${correlationId}] not found, skipped")
                        .stringify());
                } else {
                    if (logResult != null && logResult.ready(LOG, "debug")) LOG.debug(logResult
                        .put("correlationId", taskId)
                        .text("initCallbackConsumer() - push Message object to task[${correlationId}]")
                        .stringify());
                    OpflowMessage message = new OpflowMessage(content, properties.getHeaders());
                    task.push(message);
                    if (logResult != null && logResult.ready(LOG, "debug")) LOG.debug(logResult
                        .put("correlationId", taskId)
                        .text("initCallbackConsumer() - returned value of task[${correlationId}]")
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
                    if (responseDurable != null) opts.put("durable", responseDurable);
                    if (responseExclusive != null) opts.put("exclusive", responseExclusive);
                    if (responseAutoDelete != null) opts.put("autoDelete", responseAutoDelete);
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
    
    public OpflowRpcRequest request(final String routineId, byte[] body, Map<String, Object> options) {
        ReentrantReadWriteLock.ReadLock rl = pushLock.readLock();
        try {
            rl.lock();
            return _request(routineId, body, options);
        }
        finally {
            rl.unlock();
        }
    }
    
    private OpflowRpcRequest _request(final String routineId, byte[] body, Map<String, Object> options) {

        options = OpflowUtil.ensureNotNull(options);
        
        Object requestIdVal = options.get("requestId");
        if (requestIdVal == null) {
            options.put("requestId", requestIdVal = OpflowUtil.getLogID());
        }
        final String requestId = requestIdVal.toString();
        
        final OpflowLogTracer logRequest = logTracer.branch("requestId", requestId, new OpflowLogTracer.OmitPingLogs(options));
        
        if (routineId != null) {
            options.put("routineId", routineId);
        }
        
        if (expiration > 0) {
            options.put("timeout", expiration + DELAY_TIMEOUT);
        }
        
        if (timeoutMonitor == null) {
            timeoutMonitor = initTimeoutMonitor();
        }
        
        final boolean forked = "forked".equals((String)options.get("mode"));
        final OpflowEngine.ConsumerInfo consumerInfo;
        if (forked) {
            consumerInfo = initCallbackConsumer(true);
        } else {
            if (callbackConsumer == null) {
                callbackConsumer = initCallbackConsumer(false);
            }
            consumerInfo = callbackConsumer;
        }
        
        final String taskId = OpflowUtil.getLogID();
        OpflowRpcRequest task = new OpflowRpcRequest(options, new OpflowTask.Listener() {
            private OpflowLogTracer logTask = null;
            
            {
                if (logRequest != null && logRequest.ready(LOG, "debug")) logTask = logRequest.copy();
            }
            
            @Override
            public void handleEvent() {
                lock.lock();
                try {
                    tasks.remove(taskId);
                    if (forked) {
                        engine.cancelConsumer(consumerInfo);
                    }
                    if (tasks.isEmpty()) {
                        idle.signal();
                    }
                    if (logTask != null && logTask.ready(LOG, "debug")) LOG.debug(logTask
                            .put("taskId", taskId)
                            .put("taskListSize", tasks.size())
                            .text("Request[${requestId}] - RpcMaster[${rpcMasterId}]"
                                    + "- tasksize after removing task[${taskId}]: ${taskListSize}")
                            .stringify());
                } finally {
                    lock.unlock();
                }
            }
        });
        tasks.put(taskId, task);
        
        Map<String, Object> headers = new HashMap<>();
        headers.put("requestId", task.getRequestId());
        headers.put("routineId", task.getRoutineId());
        
        if (options.containsKey("messageScope")) {
            headers.put("messageScope", options.get("messageScope"));
        }
        
        if (options.get("progressEnabled") instanceof Boolean) {
            headers.put("progressEnabled", options.get("progressEnabled"));
        }
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .correlationId(taskId);
        
        if (!consumerInfo.isFixedQueue()) {
            if (logRequest != null && logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                    .put("replyTo", consumerInfo.getQueueName())
                    .text("Request[${requestId}] - RpcMaster[${rpcMasterId}] - Use dynamic replyTo: ${replyTo}")
                    .stringify());
        } else {
            if (logRequest != null && logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                    .put("replyTo", consumerInfo.getQueueName())
                    .text("Request[${requestId}] - RpcMaster[${rpcMasterId}] - Use static replyTo: ${replyTo}")
                    .stringify());
        }
        builder.replyTo(consumerInfo.getQueueName());
        
        if (expiration > 0) {
            builder.expiration(String.valueOf(expiration));
        }
        
        exporter.incRpcInvocationEvent("rpc_master", rpcMasterId, routineId, "request");
        
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
    
    private class PauseThread extends Thread {
        private final ReentrantReadWriteLock rwlock;
        private final OpflowLogTracer tracer;
        private long duration = 0;
        private long count = 0;
        private boolean running = true;
        
        public void init(long duration) {
            this.duration = duration;
            this.count = 0;
            this.running = true;
        }
        
        public void terminate() {
            running = false;
        }
        
        PauseThread(OpflowLogTracer logTracer, ReentrantReadWriteLock pushLock) {
            this.rwlock = pushLock;
            this.tracer = logTracer.copy();
            if (tracer.ready(LOG, "trace")) LOG.trace(tracer
                    .text("PauseThread[${rpcMasterId}] constructed")
                    .stringify());
        }
        
        @Override
        public void run() {
            if (duration <= 0) return;
            if (rwlock.isWriteLocked()) return;
            rwlock.writeLock().lock();
            try {
                if(rwlock.isWriteLockedByCurrentThread()) {
                    if (tracer.ready(LOG, "trace")) LOG.trace(tracer
                            .put("duration", duration)
                            .text("PauseThread[${rpcMasterId}].run() sleeping in ${duration} ms")
                            .stringify());
                    while (running && count < duration) {
                        count += 1000;
                        Thread.sleep(1000);
                    }
                }
            }
            catch (InterruptedException e) {
                if (tracer.ready(LOG, "trace")) LOG.trace(tracer
                        .text("PauseThread[${rpcMasterId}].run() is interrupted")
                        .stringify());
            }
            finally {
                if (tracer.ready(LOG, "trace")) LOG.trace(tracer
                        .text("PauseThread[${rpcMasterId}].run() wake-up")
                        .stringify());
                if(rwlock.isWriteLockedByCurrentThread()) {
                    if (tracer.ready(LOG, "trace")) LOG.trace(tracer
                            .text("PauseThread[${rpcMasterId}].run() done!")
                            .stringify());
                    rwlock.writeLock().unlock();
                }
            }
        }
    }
    
    private PauseThread pauseThread;
    
    public Map<String, Object> pause(final long duration) {
        if (pauseThread == null) {
            pauseThread = new PauseThread(logTracer, pushLock);
        }
        pauseThread.init(duration);
        threadExecutor.execute(pauseThread);
        return OpflowUtil.buildOrderedMap()
                .put("duration", duration)
                .toMap();
    }
    
    public Map<String, Object> unpause() {
        if (pauseThread != null) {
            pauseThread.terminate();
        }
        return null;
    }
    
    @Override
    public void close() {
        lock.lock();
        pushLock.writeLock().lock();
        if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - obtain the lock")
                .stringify());
        try {
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - check tasks.isEmpty()? and await...")
                .stringify());
            while(!tasks.isEmpty()) idle.await();
            
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - close CallbackConsumer")
                .stringify());
            if (callbackConsumer != null) {
                engine.cancelConsumer(callbackConsumer);
                callbackConsumer = null;
            }
            
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - stop timeoutMonitor")
                .stringify());
            if (timeoutMonitor != null) {
                timeoutMonitor.stop();
                timeoutMonitor = null;
            }
            
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - close broker/engine")
                .stringify());
            if (engine != null) engine.close();
        } catch(InterruptedException ex) {
            if (logTracer.ready(LOG, "error")) LOG.error(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - an interruption has been raised")
                .stringify());
        } finally {
            if(pushLock.isWriteLockedByCurrentThread()) {
                pushLock.writeLock().unlock();
            }
            lock.unlock();
            if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - lock has been released")
                .stringify());
        }
    }
    
    public OpflowExecutor getExecutor() {
        return executor;
    }
    
    public OpflowEngine getEngine() {
        return engine;
    }
    
    public String getInstanceId() {
        return rpcMasterId;
    }
    
    public long getExpiration() {
        return expiration;
    }
    
    public String getCallbackName() {
        return responseName;
    }
    
    public Boolean getCallbackDurable() {
        return responseDurable;
    }

    public Boolean getCallbackExclusive() {
        return responseExclusive;
    }

    public Boolean getCallbackAutoDelete() {
        return responseAutoDelete;
    }

    @Override
    protected void finalize() throws Throwable {
        exporter.changeComponentInstance("rpc_master", rpcMasterId, OpflowExporter.GaugeAction.DEC);
    }
}
