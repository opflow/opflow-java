package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.exception.OpflowRestrictionException;
import com.devebot.opflow.supports.OpflowConcurrentMap;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcMaster implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcMaster.class);
    
    private final static long DELAY_TIMEOUT = 1000;
    private final static int PREFETCH_NUM = 1;
    private final static int CONSUMER_MAX = 1;
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowRpcObserver.Listener rpcObserver;;
    private final OpflowRestrictor.Valve restrictor;
    
    private final Timer timer = new Timer("Timer-" + OpflowRpcMaster.class.getSimpleName(), true);
    private final ReentrantReadWriteLock taskLock = new ReentrantReadWriteLock();
    private final Lock closeLock = taskLock.writeLock();
    private final Condition closeBarrier = closeLock.newCondition();
    private final boolean useDuplexLock = true;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final long expiration;
    private final String responseName;
    private final Boolean responseDurable;
    private final Boolean responseExclusive;
    private final Boolean responseAutoDelete;
    private final Integer prefetchCount;
    
    private final boolean monitorEnabled;
    private final String monitorId;
    private final int monitorInterval;
    private final long monitorTimeout;
    
    private final boolean autorun;
    
    public OpflowRpcMaster(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowUtil.ensureNotNull(params);
        
        componentId = OpflowUtil.getOptionField(params, CONST.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(params, CONST.COMPNAME_MEASURER, OpflowPromMeasurer.NULL);
        rpcObserver = (OpflowRpcObserver.Listener) OpflowUtil.getOptionField(params, CONST.COMPNAME_RPC_OBSERVER, null);
        restrictor = new OpflowRestrictor.Valve();
        
        logTracer = OpflowLogTracer.ROOT.branch("rpcMasterId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcMaster[${rpcMasterId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put(CONST.COMPONENT_ID, componentId);
        brokerParams.put(CONST.COMPNAME_MEASURER, measurer);
        brokerParams.put("mode", "rpc_master");
        brokerParams.put("exchangeType", "direct");
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        engine.setProducingBlockedListener(new BlockedListener() {
            @Override
            public void handleBlocked(String reason) throws IOException {
                if (restrictor != null) {
                    restrictor.block();
                }
            }

            @Override
            public void handleUnblocked() throws IOException {
                if (restrictor != null) {
                    restrictor.unblock();
                }
            }
        });
        
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
                responseQueueSuffix = OpflowUUID.getUUID();
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
        
        if (params.get("prefetchCount") != null && params.get("prefetchCount") instanceof Integer) {
            prefetchCount = (Integer) params.get("prefetchCount");
        } else {
            prefetchCount = PREFETCH_NUM;
        }
        
        if (responseName != null) {
            executor.assertQueue(responseName, responseDurable, responseExclusive, responseAutoDelete);
        }
        
        if (params.get("monitorEnabled") != null && params.get("monitorEnabled") instanceof Boolean) {
            monitorEnabled = (Boolean) params.get("monitorEnabled");
        } else {
            monitorEnabled = true;
        }
        
        monitorId = params.get("monitorId") instanceof String ? (String)params.get("monitorId") : componentId;
        
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
        
        if (params.get("autorun") instanceof Boolean) {
            autorun = (Boolean) params.get("autorun");
        } else {
            autorun = false;
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("autorun", autorun)
                .put("responseName", responseName)
                .put("responseDurable", responseDurable)
                .put("responseExclusive", responseExclusive)
                .put("responseAutoDelete", responseAutoDelete)
                .put("prefetchCount", prefetchCount)
                .put("monitorId", monitorId)
                .put("monitorEnabled", monitorEnabled)
                .put("monitorInterval", monitorInterval)
                .put("monitorTimeout", monitorTimeout)
                .tags("RpcMaster.new() parameters")
                .text("RpcMaster[${rpcMasterId}].new() parameters")
                .stringify());
        
        measurer.updateComponentInstance("rpc_master", componentId, OpflowPromMeasurer.GaugeAction.INC);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcMaster[${rpcMasterId}].new() end!")
                .stringify());
        
        if (autorun) {
            this.serve();
        }
    }

    private final OpflowConcurrentMap<String, OpflowRpcRequest> tasks = new OpflowConcurrentMap<>();
    
    private final Object callbackConsumerLock = new Object();
    private volatile OpflowEngine.ConsumerInfo callbackConsumer;

    private OpflowEngine.ConsumerInfo initCallbackConsumer(final boolean isTransient) {
        final String _consumerId = OpflowUUID.getBase64ID();
        final OpflowLogTracer logSession = logTracer.branch("consumerId", _consumerId);
        if (logSession.ready(LOG, Level.INFO)) LOG.info(logSession
                .put("isTransient", isTransient)
                .text("initCallbackConsumer() is invoked (isTransient: ${isTransient})")
                .stringify());
        return engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(
                    byte[] content,
                    AMQP.BasicProperties properties,
                    String queueName,
                    Channel channel,
                    String consumerTag,
                    Map<String, Object> extras
            ) throws IOException {
                String taskId = properties.getCorrelationId();
                Map<String, Object> headers = properties.getHeaders();
                
                OpflowLogTracer reqTracer = null;
                
                if (extras != null && extras.containsKey(CONST.REQUEST_TRACER_NAME)) {
                    reqTracer = (OpflowLogTracer) extras.get(CONST.REQUEST_TRACER_NAME);
                }
                
                if (reqTracer == null) {
                    String requestId = OpflowUtil.getRequestId(headers);
                    String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);

                    if (logSession.ready(LOG, Level.INFO)) {
                        reqTracer = logSession.branch(CONST.REQUEST_TIME, routineTimestamp)
                                .branch(CONST.REQUEST_ID, requestId, new OpflowLogTracer.OmitPingLogs(headers));
                    }
                }
                
                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                        .put("correlationId", taskId)
                        .put("bodyLength", (content != null ? content.length : -1))
                        .text("initCallbackConsumer() - task[${correlationId}] receives a result (size: ${bodyLength})")
                        .stringify());

                OpflowRpcRequest task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (reqTracer != null && reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                        .put("correlationId", taskId)
                        .text("initCallbackConsumer() - task[${correlationId}] not found, skipped")
                        .stringify());
                } else {
                    if (reqTracer != null && reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                        .put("correlationId", taskId)
                        .text("initCallbackConsumer() - push message to task[${correlationId}] and return")
                        .stringify());
                    task.push(new OpflowMessage(content, properties.getHeaders()));
                }
                
                // collect the information of the workers
                if (rpcObserver != null) {
                    String rpcWorkerId = OpflowUtil.getStringField(headers, CONST.RPC_WORKER_ID, false, true);
                    rpcObserver.check(rpcWorkerId);
                }
                
                return true;
            }
        }, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("consumerId", _consumerId);
                if (!isTransient) {
                    opts.put("queueName", responseName);
                    if (responseDurable != null) opts.put("durable", responseDurable);
                    if (responseExclusive != null) opts.put("exclusive", responseExclusive);
                    if (responseAutoDelete != null) opts.put("autoDelete", responseAutoDelete);
                    opts.put("consumerLimit", CONSUMER_MAX);
                    opts.put("forceNewChannel", Boolean.FALSE);
                    opts.put("reqTracerShared", Boolean.TRUE);
                }
                opts.put("binding", Boolean.FALSE);
                opts.put("prefetchCount", prefetchCount);
            }
        }).toMap());
    }
    
    private void cancelCallbackConsumer() {
        synchronized (callbackConsumerLock) {
            if (callbackConsumer != null) {
                engine.cancelConsumer(callbackConsumer);
                callbackConsumer = null;
            }
        }
    }
    
    private final Object timeoutMonitorLock = new Object();
    private OpflowTimeout.Monitor timeoutMonitor = null;
    
    private OpflowTimeout.Monitor initTimeoutMonitor() {
        OpflowTimeout.Monitor monitor = null;
        if (monitorEnabled) {
            monitor = new OpflowTimeout.Monitor(tasks, monitorInterval, monitorTimeout, monitorId);
            monitor.start();
        }
        return monitor;
    }
    
    public OpflowRpcRequest request(String routineSignature, String body) {
        return request(routineSignature, OpflowUtil.getBytes(body), null, null);
    }
    
    public OpflowRpcRequest request(String routineSignature, String body, Map<String, Object> options) {
        return request(routineSignature, OpflowUtil.getBytes(body), null, options);
    }
    
    public OpflowRpcRequest request(String routineSignature, String body, final OpflowRpcParameter params) {
        return request(routineSignature, OpflowUtil.getBytes(body), params, null);
    }
    
    public OpflowRpcRequest request(String routineSignature, byte[] body) {
        return request(routineSignature, body, null, null);
    }
    
    public OpflowRpcRequest request(final String routineSignature, final byte[] body, final OpflowRpcParameter params) {
        return request(routineSignature, body, params, null);
    }
    
    public OpflowRpcRequest request(final String routineSignature, final byte[] body, final Map<String, Object> options) {
        return request(routineSignature, body, null, options);
    }
    
    public OpflowRpcRequest request(final String routineSignature, final byte[] body, final OpflowRpcParameter params, final Map<String, Object> options) {
        if (restrictor == null) {
            return _request_safe(routineSignature, body, params, options);
        }
        try {
            return restrictor.filter(new OpflowRestrictor.Action<OpflowRpcRequest>() {
                @Override
                public OpflowRpcRequest process() throws Throwable {
                    return _request_safe(routineSignature, body, params, options);
                }
            });
        }
        catch (OpflowOperationException opflowException) {
            throw opflowException;
        }
        catch (Throwable e) {
            throw new OpflowRestrictionException(e);
        }
    }
    
    private OpflowRpcRequest _request_safe(final String routineSignature, byte[] body, OpflowRpcParameter parameter, Map<String, Object> options) {
        final OpflowRpcParameter params = (parameter != null) ? parameter : new OpflowRpcParameter(options);
        
        if (routineSignature != null) {
            params.setRoutineSignature(routineSignature);
        }
        
        if (expiration > 0) {
            params.setRequestTTL(expiration + DELAY_TIMEOUT);
        }
        
        final OpflowLogTracer reqTracer = logTracer.branch(CONST.REQUEST_TIME, params.getRoutineTimestamp())
                .branch(CONST.REQUEST_ID, params.getRequestId(), params);
        
        if (timeoutMonitor == null) {
            synchronized(timeoutMonitorLock) {
                if (timeoutMonitor == null) {
                    timeoutMonitor = initTimeoutMonitor();
                }
            }
        }
        
        final OpflowEngine.ConsumerInfo consumerInfo;
        if (params.getCallbackTransient()) {
            consumerInfo = initCallbackConsumer(true);
        } else {
            if (callbackConsumer == null) {
                synchronized(callbackConsumerLock) {
                    if (callbackConsumer == null) {
                        callbackConsumer = initCallbackConsumer(false);
                    }
                }
            }
            consumerInfo = callbackConsumer;
        }
        
        final String taskId = OpflowUUID.getBase64ID();
        OpflowRpcRequest task = new OpflowRpcRequest(params, new OpflowTimeout.Listener() {
            private OpflowLogTracer logTask = null;
            
            {
                if (reqTracer != null && reqTracer.ready(LOG, Level.DEBUG)) {
                    logTask = reqTracer.branch("taskId", taskId);
                }
            }
            
            @Override
            public void handleEvent() {
                Lock eventLock = useDuplexLock ? taskLock.readLock() : closeLock;
                eventLock.lock();
                try {
                    tasks.remove(taskId);
                    if (params.getCallbackTransient()) {
                        engine.cancelConsumer(consumerInfo);
                    }
                    if (tasks.isEmpty()) {
                        try {
                            if (eventLock != closeLock) {
                                eventLock.unlock();
                                closeLock.lock();
                            }
                            closeBarrier.signal();
                        }
                        finally {
                            if (eventLock != closeLock) {
                                closeLock.unlock();
                                eventLock.lock();
                            }
                        }
                    }
                    if (logTask != null && logTask.ready(LOG, Level.DEBUG)) LOG.debug(logTask
                            .put("taskListSize", tasks.size())
                            .text("Request[${requestId}][${requestTime}][x-rpc-master-finished] - RpcMaster[${rpcMasterId}]"
                                    + "- tasksize after removing task[${taskId}]: ${taskListSize}")
                            .stringify());
                } finally {
                    eventLock.unlock();
                }
            }
        });
        tasks.put(taskId, task);
        
        Map<String, Object> headers = new HashMap<>();
        headers.put(CONST.REQUEST_ID, task.getRequestId());
        headers.put(CONST.AMQP_HEADER_ROUTINE_SIGNATURE, task.getRoutineSignature());
        headers.put(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, task.getRoutineTimestamp());
        
        if (params.getRoutineTags() != null) {
            headers.put(CONST.AMQP_HEADER_ROUTINE_TAGS, params.getRoutineTags());
        }
        
        if (params.getMessageScope() != null) {
            headers.put("messageScope", params.getMessageScope());
        }
        
        if (prefetchCount > 1) {
            headers.put("progressEnabled", Boolean.FALSE);
        } else {
            if (params.getProgressEnabled() != null) {
                headers.put("progressEnabled", params.getProgressEnabled());
            }
        }
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .correlationId(taskId);
        
        if (!consumerInfo.isFixedQueue()) {
            if (reqTracer != null && reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                    .put("replyTo", consumerInfo.getQueueName())
                    .text("Request[${requestId}][${requestTime}][x-rpc-master-request] - RpcMaster[${rpcMasterId}] - Use dynamic replyTo: ${replyTo}")
                    .stringify());
        } else {
            if (reqTracer != null && reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                    .put("replyTo", consumerInfo.getQueueName())
                    .text("Request[${requestId}][${requestTime}][x-rpc-master-request] - RpcMaster[${rpcMasterId}] - Use static replyTo: ${replyTo}")
                    .stringify());
        }
        builder.replyTo(consumerInfo.getQueueName());
        
        if (expiration > 0) {
            builder.expiration(String.valueOf(expiration));
        }
        
        measurer.countRpcInvocation("rpc_master", "request", routineSignature, "begin");
        
        engine.produce(body, headers, builder, null, reqTracer);
        
        return task;
    }
    
    public int getActiveRequestTotal() {
        return tasks.size();
    }
    
    public int getMaxWaitingRequests() {
        return tasks.getMaxSize();
    }
    
    public void resetCallbackQueueCounter() {
        tasks.resetMaxSize();
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
    
    private void scheduleClearTasks() {
        final OpflowLogTracer localLog = logTracer.copy();
        if (!tasks.isEmpty()) {
            if (localLog.ready(LOG, Level.TRACE)) LOG.trace(localLog
                    .text("RpcMaster[${rpcMasterId}].close() - schedule the clean jobs")
                    .stringify());
            // prevent from receiving the callback RPC messages
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (localLog.ready(LOG, Level.TRACE)) LOG.trace(localLog
                            .text("RpcMaster[${rpcMasterId}].close() - force cancelCallbackConsumer")
                            .stringify());
                    cancelCallbackConsumer();
                }
            }, (DELAY_TIMEOUT));
            // clear the callback request list
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (localLog.ready(LOG, Level.TRACE)) LOG.trace(localLog
                            .text("RpcMaster[${rpcMasterId}].close() - force clear callback list")
                            .stringify());
                    closeLock.lock();
                    try {
                        tasks.clear();
                        closeBarrier.signal();
                    }
                    finally {
                        closeLock.unlock();
                    }
                }
            }, (DELAY_TIMEOUT + DELAY_TIMEOUT));
        } else {
            if (localLog.ready(LOG, Level.TRACE)) LOG.trace(localLog
                    .text("RpcMaster[${rpcMasterId}].close() - request callback list is empty")
                    .stringify());
        }
    }
    
    private void completeClearTasks() {
        timer.cancel();
        timer.purge();
    }
    
    public final void serve() {
        if (restrictor != null) {
            restrictor.unblock();
        }
    }
    
    @Override
    public void close() {
        if (restrictor != null) {
            restrictor.block();
        }
        closeLock.lock();
        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - obtain the lock")
                .stringify());
        try {
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - check tasks.isEmpty()? and await...")
                .stringify());
            
            scheduleClearTasks();
            
            while(!tasks.isEmpty()) closeBarrier.await();
            
            completeClearTasks();
            
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - cancelCallbackConsumer (for sure)")
                .stringify());
            
            cancelCallbackConsumer();
            
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - stop timeoutMonitor")
                .stringify());

            synchronized (timeoutMonitorLock) {
                if (timeoutMonitor != null) {
                    timeoutMonitor.close();
                    timeoutMonitor = null;
                }
            }
            
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - close broker/engine")
                .stringify());
            if (engine != null) engine.close();
        } catch(InterruptedException ex) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - an interruption has been raised")
                .stringify());
        } finally {
            closeLock.unlock();
            if (autorun) {
                if (restrictor != null) {
                    restrictor.unblock();
                }
            }
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("RpcMaster[${rpcMasterId}].close() - lock has been released")
                .stringify());
        }
    }
    
    public void reset() {
        close();
        serve();
    }
    
    public OpflowExecutor getExecutor() {
        return executor;
    }
    
    public OpflowEngine getEngine() {
        return engine;
    }
    
    public String getComponentId() {
        return componentId;
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
        measurer.updateComponentInstance("rpc_master", componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
