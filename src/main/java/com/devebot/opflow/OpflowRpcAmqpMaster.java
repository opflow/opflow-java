package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.exception.OpflowRestrictionException;
import com.devebot.opflow.supports.OpflowConcurrentMap;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.rabbitmq.nostro.client.AMQP;
import com.rabbitmq.nostro.client.BlockedListener;
import com.rabbitmq.nostro.client.Channel;
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
public class OpflowRpcAmqpMaster implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcAmqpMaster.class);

    private final static long DELAY_TIMEOUT = 1000;
    private final static int PREFETCH_NUM = 1;
    private final static int CONSUMER_MAX = 1;
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowRpcObserver rpcObserver;
    private final OpflowRestrictor.Valve restrictor;
    
    private final Timer timer = new Timer("Timer-" + OpflowRpcAmqpMaster.class.getSimpleName(), true);
    private final ReentrantReadWriteLock taskLock = new ReentrantReadWriteLock();
    private final Lock closeLock = taskLock.writeLock();
    private final Condition closeBarrier = closeLock.newCondition();
    private final boolean useDuplexLock = true;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final long expiration;
    private final String responseQueueName;
    private final Boolean responseQueueDurable;
    private final Boolean responseQueueExclusive;
    private final Boolean responseQueueAutoDelete;
    private final Integer responsePrefetchCount;
    
    private final boolean monitorEnabled;
    private final String monitorId;
    private final int monitorInterval;
    private final long monitorTimeout;
    
    private final boolean autorun;
    
    public OpflowRpcAmqpMaster(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowObjectTree.ensureNonNull(params);
        
        componentId = OpflowUtil.getStringField(params, CONST.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(params, OpflowConstant.COMP_MEASURER, OpflowPromMeasurer.NULL);
        rpcObserver = (OpflowRpcObserver) OpflowUtil.getOptionField(params, OpflowConstant.COMP_RPC_OBSERVER, null);
        restrictor = new OpflowRestrictor.Valve();
        
        logTracer = OpflowLogTracer.ROOT.branch("amqpMasterId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("amqpMaster[${amqpMasterId}][${instanceId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        
        brokerParams.put(CONST.COMPONENT_ID, componentId);
        brokerParams.put(OpflowConstant.COMP_MEASURER, measurer);
        brokerParams.put(OpflowConstant.OPFLOW_COMMON_INSTANCE_OWNER, OpflowConstant.COMP_RPC_AMQP_MASTER);
        
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME, params.get(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME));
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_TYPE, params.getOrDefault(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_TYPE, "direct"));
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_DURABLE, params.get(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_DURABLE));
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY, params.get(OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY));
        
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
        
        // Message TTL option
        expiration = OpflowUtil.getLongField(params, OpflowConstant.AMQP_PARAM_MESSAGE_TTL, 0l);
        
        // Response queue options
        String responseQueuePattern = OpflowUtil.getStringField(params, OpflowConstant.OPFLOW_RESPONSE_QUEUE_SUFFIX);
        String responseQueueSuffix = null;
        if (responseQueuePattern != null && responseQueuePattern.length() > 0) {
            if (responseQueuePattern.equals("~")) {
                responseQueueSuffix = OpflowUUID.getUUID();
            } else {
                responseQueueSuffix = responseQueuePattern;
            }
        }
        
        String _responseQueueName = OpflowUtil.getStringField(params, OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME);
        if (_responseQueueName != null) {
            responseQueueName = responseQueueSuffix != null ? _responseQueueName + '_' + responseQueueSuffix : _responseQueueName;
        } else {
            responseQueueName = null;
        }
        
        responseQueueDurable = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_RESPONSE_QUEUE_DURABLE, responseQueueSuffix != null ? false : null);
        responseQueueExclusive = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_RESPONSE_QUEUE_EXCLUSIVE, responseQueueSuffix != null ? true : null);
        responseQueueAutoDelete = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_RESPONSE_QUEUE_AUTO_DELETE, responseQueueSuffix != null ? true : null);
        responsePrefetchCount = OpflowUtil.getIntegerField(params, OpflowConstant.OPFLOW_RESPONSE_PREFETCH_COUNT, PREFETCH_NUM);
        
        if (responseQueueName != null) {
            executor.assertQueue(responseQueueName, responseQueueDurable, responseQueueExclusive, responseQueueAutoDelete);
        }
        
        // Auto-binding section
        String _dispatchQueueName = OpflowUtil.getStringField(params, OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME);
        if (_dispatchQueueName != null) {
            Boolean _dispatchQueueDurable = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE, null);
            Boolean _dispatchQueueExclusive = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE, null);
            Boolean _dispatchQueueAutoDelete = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE, null);
            executor.assertQueue(_dispatchQueueName, _dispatchQueueDurable, _dispatchQueueExclusive, _dispatchQueueAutoDelete);
            executor.bindExchange(engine.getExchangeName(), engine.getRoutingKey(), _dispatchQueueName);
        }
        
        // RPC Monitor section
        monitorEnabled = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_RPC_MONITOR_ENABLED, true);
        monitorId = OpflowUtil.getStringField(params, OpflowConstant.OPFLOW_RPC_MONITOR_ID, componentId);
        monitorInterval = OpflowUtil.getIntegerField(params, OpflowConstant.OPFLOW_RPC_MONITOR_INTERVAL, 14000); // can run 2-3 times in 30s
        monitorTimeout = OpflowUtil.getLongField(params, OpflowConstant.OPFLOW_RPC_MONITOR_TIMEOUT, 0l);
        
        // Autorun section
        autorun = OpflowUtil.getBooleanField(params, OpflowConstant.OPFLOW_COMMON_AUTORUN, Boolean.FALSE);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("autorun", autorun)
                .put("responseName", responseQueueName)
                .put("responseDurable", responseQueueDurable)
                .put("responseExclusive", responseQueueExclusive)
                .put("responseAutoDelete", responseQueueAutoDelete)
                .put("prefetchCount", responsePrefetchCount)
                .put("monitorId", monitorId)
                .put("monitorEnabled", monitorEnabled)
                .put("monitorInterval", monitorInterval)
                .put("monitorTimeout", monitorTimeout)
                .tags("RpcAmqpMaster.new() parameters")
                .text("amqpMaster[${amqpMasterId}].new() parameters")
                .stringify());
        
        measurer.updateComponentInstance(OpflowConstant.COMP_RPC_AMQP_MASTER, componentId, OpflowPromMeasurer.GaugeAction.INC);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("amqpMaster[${amqpMasterId}][${instanceId}].new() end!")
                .stringify());
        
        if (autorun) {
            this.serve();
        }
    }
    
    private final OpflowConcurrentMap<String, OpflowRpcAmqpRequest> tasks = new OpflowConcurrentMap<>();
    
    private final Object callbackConsumerLock = new Object();
    private volatile OpflowEngine.ConsumerInfo callbackConsumer;
    
    private OpflowEngine.ConsumerInfo initCallbackConsumer(final boolean isTransient) {
        final String _consumerId = OpflowUUID.getBase64ID();
        final OpflowLogTracer logSession = logTracer.branch("consumerId", _consumerId);
        if (logSession.ready(LOG, Level.INFO)) LOG.info(logSession
                .put("isTransient", isTransient)
                .text("initCallbackConsumer() is invoked (isTransient: ${isTransient})")
                .stringify());
        return engine.consume(new OpflowEngine.Listener() {
            @Override
            public boolean processMessage(
                    byte[] content,
                    AMQP.BasicProperties properties,
                    String queueName,
                    Channel channel,
                    String consumerTag,
                    Map<String, String> extras
            ) throws IOException {
                String taskId = properties.getCorrelationId();
                Map<String, Object> headers = properties.getHeaders();
                
                if (extras == null) {
                    extras = new HashMap<>();
                }
                
                String routineId = extras.get(CONST.AMQP_HEADER_ROUTINE_ID);
                String routineTimestamp = extras.get(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP);
                String routineScope = extras.get(CONST.AMQP_HEADER_ROUTINE_SCOPE);
                
                if (routineId == null) routineId = OpflowUtil.getRoutineId(headers);
                if (routineTimestamp == null) routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);
                if (routineScope == null) routineScope = OpflowUtil.getRoutineScope(headers);
                
                OpflowLogTracer reqTracer = null;
                if (logSession.ready(LOG, Level.INFO)) {
                    reqTracer = logSession.branch(CONST.REQUEST_TIME, routineTimestamp)
                            .branch(CONST.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(routineScope));
                }
                
                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                        .put("correlationId", taskId)
                        .put("bodyLength", (content != null ? content.length : -1))
                        .text("Request[${requestId}][${requestTime}][x-rpc-master-callback-consumed] - task[${correlationId}] receives a result (size: ${bodyLength})")
                        .stringify());

                OpflowRpcAmqpRequest task = tasks.get(taskId);
                if (taskId == null || task == null) {
                    if (reqTracer != null && reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                        .put("correlationId", taskId)
                        .text("Request[${requestId}][${requestTime}][x-rpc-master-callback-skipped] - task[${correlationId}] not found, skipped")
                        .stringify());
                } else {
                    if (reqTracer != null && reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                        .put("correlationId", taskId)
                        .text("Request[${requestId}][${requestTime}][x-rpc-master-callback-finished] - push message to task[${correlationId}] and return")
                        .stringify());
                    task.push(new OpflowEngine.Message(content, headers));
                }
                
                // collect the information of the workers
                if (rpcObserver != null) {
                    rpcObserver.check(OpflowConstant.Protocol.AMQP, headers);
                }
                
                return true;
            }
        }, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_ID, _consumerId);
                if (!isTransient) {
                    opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME, responseQueueName);
                    opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_DURABLE, responseQueueDurable);
                    opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_EXCLUSIVE, responseQueueExclusive);
                    opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_AUTO_DELETE, responseQueueAutoDelete);
                    opts.put(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_LIMIT, CONSUMER_MAX);
                }
                opts.put(OpflowConstant.OPFLOW_CONSUMING_AUTO_BINDING, Boolean.FALSE);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT, responsePrefetchCount);
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
            monitor.serve();
        }
        return monitor;
    }
    
    public OpflowRpcAmqpRequest request(String routineSignature, String body) {
        return request(routineSignature, OpflowUtil.getBytes(body), null, null);
    }
    
    public OpflowRpcAmqpRequest request(String routineSignature, String body, OpflowRpcParameter params) {
        return request(routineSignature, OpflowUtil.getBytes(body), params, null);
    }

    public OpflowRpcAmqpRequest request(String routineSignature, String body, OpflowRpcRoutingInfo routingInfo) {
        return request(routineSignature, OpflowUtil.getBytes(body), null, routingInfo);
    }
    
    public OpflowRpcAmqpRequest request(String routineSignature, byte[] body) {
        return request(routineSignature, body, null, null);
    }
    
    public OpflowRpcAmqpRequest request(String routineSignature, byte[] body, OpflowRpcParameter params) {
        return request(routineSignature, body, params, null);
    }
    
    public OpflowRpcAmqpRequest request(String routineSignature, byte[] body, OpflowRpcRoutingInfo routingInfo) {
        return request(routineSignature, body, null, routingInfo);
    }
    
    public OpflowRpcAmqpRequest request(final String routineSignature, final byte[] body, final OpflowRpcParameter params, final OpflowRpcRoutingInfo routingInfo) {
        if (restrictor == null) {
            return _request_safe(routineSignature, body, params, routingInfo);
        }
        try {
            return restrictor.filter(new OpflowRestrictor.Action<OpflowRpcAmqpRequest>() {
                @Override
                public OpflowRpcAmqpRequest process() throws Throwable {
                    return _request_safe(routineSignature, body, params, routingInfo);
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
    
    private OpflowRpcAmqpRequest _request_safe(final String routineSignature, byte[] body, OpflowRpcParameter parameter, OpflowRpcRoutingInfo routingInfo) {
        final OpflowRpcParameter params = (parameter != null) ? parameter : new OpflowRpcParameter();
        
        if (routineSignature != null) {
            params.setRoutineSignature(routineSignature);
        }
        
        if (expiration > 0) {
            params.setRoutineTTL(expiration + DELAY_TIMEOUT);
        }
        
        final OpflowLogTracer reqTracer = logTracer.branch(CONST.REQUEST_TIME, params.getRoutineTimestamp())
                .branch(CONST.REQUEST_ID, params.getRoutineId(), params);
        
        if (timeoutMonitor == null) {
            synchronized (timeoutMonitorLock) {
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
                synchronized (callbackConsumerLock) {
                    if (callbackConsumer == null) {
                        callbackConsumer = initCallbackConsumer(false);
                    }
                }
            }
            consumerInfo = callbackConsumer;
        }
        
        final String taskId = OpflowUUID.getBase64ID();
        OpflowRpcAmqpRequest task = new OpflowRpcAmqpRequest(params, new OpflowTimeout.Listener() {
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
                            .text("Request[${requestId}][${requestTime}][x-rpc-master-finished] - amqpMaster[${amqpMasterId}]"
                                    + "- tasksize after removing task[${taskId}]: ${taskListSize}")
                            .stringify());
                } finally {
                    eventLock.unlock();
                }
            }
        });
        tasks.put(taskId, task);
        
        Map<String, Object> headers = new HashMap<>();
        OpflowUtil.setRoutineId(headers, task.getRoutineId());
        OpflowUtil.setRoutineTimestamp(headers, task.getRoutineTimestamp());
        OpflowUtil.setRoutineSignature(headers, task.getRoutineSignature());
        OpflowUtil.setRoutineScope(headers, params.getRoutineScope());
        OpflowUtil.setRoutineTags(headers, params.getRoutineTags());

        if (responsePrefetchCount > 1) {
            OpflowUtil.setProgressEnabled(headers, Boolean.FALSE);
        } else {
            OpflowUtil.setProgressEnabled(headers, params.getProgressEnabled());
        }

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .correlationId(taskId);

        if (reqTracer != null && reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                .put("replyTo", consumerInfo.getQueueName())
                .put("replyToType", consumerInfo.isFixedQueue() ? "static" : "dynamic")
                .text("Request[${requestId}][${requestTime}][x-rpc-master-request] - amqpMaster[${amqpMasterId}][${instanceId}] - Use ${replyToType} replyTo: ${replyTo}")
                .stringify());
        builder.replyTo(consumerInfo.getQueueName());

        if (expiration > 0) {
            builder.expiration(String.valueOf(expiration));
        }
        
        measurer.countRpcInvocation(OpflowConstant.COMP_RPC_AMQP_MASTER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "produce");
        
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
                    .text("amqpMaster[${amqpMasterId}].close() - schedule the clean jobs")
                    .stringify());
            // prevent from receiving the callback RPC messages
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (localLog.ready(LOG, Level.TRACE)) LOG.trace(localLog
                            .text("amqpMaster[${amqpMasterId}].close() - force cancelCallbackConsumer")
                            .stringify());
                    cancelCallbackConsumer();
                }
            }, (DELAY_TIMEOUT));
            // clear the callback request list
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (localLog.ready(LOG, Level.TRACE)) LOG.trace(localLog
                            .text("amqpMaster[${amqpMasterId}].close() - force clear callback list")
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
                    .text("amqpMaster[${amqpMasterId}].close() - request callback list is empty")
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
                .text("amqpMaster[${amqpMasterId}].close() - obtain the lock")
                .stringify());
        try {
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("amqpMaster[${amqpMasterId}].close() - check tasks.isEmpty()? and await...")
                .stringify());
            
            scheduleClearTasks();
            
            while(!tasks.isEmpty()) closeBarrier.await();
            
            completeClearTasks();
            
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("amqpMaster[${amqpMasterId}].close() - cancelCallbackConsumer (for sure)")
                .stringify());
            
            cancelCallbackConsumer();
            
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("amqpMaster[${amqpMasterId}].close() - stop timeoutMonitor")
                .stringify());

            synchronized (timeoutMonitorLock) {
                if (timeoutMonitor != null) {
                    timeoutMonitor.close();
                    timeoutMonitor = null;
                }
            }
            
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("amqpMaster[${amqpMasterId}].close() - close broker/engine")
                .stringify());
            if (engine != null) engine.close();
        } catch(InterruptedException ex) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                .text("amqpMaster[${amqpMasterId}].close() - an interruption has been raised")
                .stringify());
        } finally {
            closeLock.unlock();
            if (autorun) {
                if (restrictor != null) {
                    restrictor.unblock();
                }
            }
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                .text("amqpMaster[${amqpMasterId}].close() - lock has been released")
                .stringify());
        }
    }
    
    public void reset() {
        close();
        serve();
    }
    
    public OpflowEngine getEngine() {
        return engine;
    }
    
    public OpflowExecutor getExecutor() {
        return executor;
    }
    
    public String getComponentId() {
        return componentId;
    }
    
    public long getExpiration() {
        return expiration;
    }
    
    public String getResponseQueueName() {
        return responseQueueName;
    }
    
    public Boolean getResponseQueueAutoDelete() {
        return responseQueueAutoDelete;
    }

    public Boolean getResponseQueueDurable() {
        return responseQueueDurable;
    }

    public Boolean getResponseQueueExclusive() {
        return responseQueueExclusive;
    }

    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_RPC_AMQP_MASTER, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
