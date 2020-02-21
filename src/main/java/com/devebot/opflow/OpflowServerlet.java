package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.annotation.OpflowTargetRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowServerlet implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    public final static List<String> SERVICE_BEAN_NAMES = Arrays.asList(new String[] {
        "configurer", "rpcWorker", "subscriber"
    });

    public final static List<String> SUPPORT_BEAN_NAMES = Arrays.asList(new String[] {
        "promExporter"
    });

    public final static List<String> ALL_BEAN_NAMES = OpflowUtil.mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowServerlet.class);
    
    private final String instanceId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowConfig.Loader configLoader;
    
    private OpflowPubsubHandler configurer;
    private OpflowRpcWorker rpcWorker;
    private OpflowPubsubHandler subscriber;
    private Instantiator instantiator;
    
    private final ListenerDescriptor listenerMap;
    
    private final Map<String, Object> kwargs;
    
    public OpflowServerlet(ListenerDescriptor listeners, OpflowConfig.Loader loader) throws OpflowBootstrapException {
        this(listeners, loader, null);
    }
    
    public OpflowServerlet(ListenerDescriptor listeners, Map<String, Object> kwargs) throws OpflowBootstrapException {
        this(listeners, null, kwargs);
    }
    
    public OpflowServerlet(ListenerDescriptor listeners, OpflowConfig.Loader loader, Map<String, Object> kwargs) throws OpflowBootstrapException {
        if (loader != null) {
            configLoader = loader;
        } else {
            configLoader = null;
        }
        if (configLoader != null) {
            kwargs = configLoader.loadConfiguration();
        }
        
        this.kwargs = OpflowUtil.ensureNotNull(kwargs);
        
        instanceId = OpflowUtil.getOptionField(this.kwargs, CONST.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("serverletId", instanceId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Serverlet[${serverletId}].new()")
                .stringify());
        
        if (listeners == null) {
            throw new OpflowBootstrapException("Listener definitions must not be null");
        }
        listenerMap = listeners;
        
        measurer = OpflowPromMeasurer.getInstance((Map<String, Object>) kwargs.get("promExporter"));
        
        Map<String, Object> configurerCfg = (Map<String, Object>)this.kwargs.get("configurer");
        Map<String, Object> rpcWorkerCfg = (Map<String, Object>)this.kwargs.get("rpcWorker");
        Map<String, Object> subscriberCfg = (Map<String, Object>)this.kwargs.get("subscriber");
        
        HashSet<String> checkExchange = new HashSet<>();
        HashSet<String> checkQueue = new HashSet<>();
        HashSet<String> checkRecyclebin = new HashSet<>();
        
        if (OpflowUtil.isComponentEnabled(configurerCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(configurerCfg)) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            } 
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(configurerCfg))) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters (exchangeName-routingKey)");
            }
            if (configurerCfg.get("subscriberName") != null && !checkQueue.add(configurerCfg.get("subscriberName").toString())) {
                throw new OpflowBootstrapException("Configurer[subscriberName] must not be duplicated");
            }
            if (configurerCfg.get("recyclebinName") != null) checkRecyclebin.add(configurerCfg.get("recyclebinName").toString());
        }

        if (OpflowUtil.isComponentEnabled(rpcWorkerCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(rpcWorkerCfg)) {
                throw new OpflowBootstrapException("Invalid RpcWorker connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(rpcWorkerCfg))) {
                throw new OpflowBootstrapException("Duplicated RpcWorker connection parameters (exchangeName-routingKey)");
            }
            if (rpcWorkerCfg.get("operatorName") != null && !checkQueue.add(rpcWorkerCfg.get("operatorName").toString())) {
                throw new OpflowBootstrapException("RpcWorker[operatorName] must not be duplicated");
            }
            if (rpcWorkerCfg.get("responseName") != null && !checkQueue.add(rpcWorkerCfg.get("responseName").toString())) {
                throw new OpflowBootstrapException("RpcWorker[responseName] must not be duplicated");
            }
        }
        
        if (OpflowUtil.isComponentEnabled(subscriberCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(subscriberCfg)) {
                throw new OpflowBootstrapException("Invalid Subscriber connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(subscriberCfg))) {
                throw new OpflowBootstrapException("Duplicated Subscriber connection parameters (exchangeName-routingKey)");
            }
            if (subscriberCfg.get("subscriberName") != null && !checkQueue.add(subscriberCfg.get("subscriberName").toString())) {
                throw new OpflowBootstrapException("Subscriber[subscriberName] must not be duplicated");
            }
            if (subscriberCfg.get("recyclebinName") != null) checkRecyclebin.add(subscriberCfg.get("recyclebinName").toString());
        }
        
        checkRecyclebin.retainAll(checkQueue);
        if (!checkRecyclebin.isEmpty()) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                .text("duplicated_recyclebin_queue_name").toString());
            throw new OpflowBootstrapException("Invalid recyclebinName (duplicated with some queueNames)");
        }
        
        try {
            if (OpflowUtil.isComponentEnabled(configurerCfg)) {
                String pubsubHandlerId = OpflowUUID.getBase64ID();
                configurerCfg.put(CONST.COMPONENT_ID, pubsubHandlerId);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("pubsubHandlerId", pubsubHandlerId)
                        .text("Serverlet[${serverletId}] creates a new configurer[${pubsubHandlerId}]")
                        .stringify());
                configurer = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("measurer", measurer);
                    }
                }, configurerCfg).toMap());
            }

            if (OpflowUtil.isComponentEnabled(rpcWorkerCfg)) {
                String rpcWorkerId = OpflowUUID.getBase64ID();
                rpcWorkerCfg.put(CONST.COMPONENT_ID, rpcWorkerId);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("rpcWorkerId", rpcWorkerId)
                        .text("Serverlet[${serverletId}] creates a new rpcWorker[${rpcWorkerId}]")
                        .stringify());
                rpcWorker = new OpflowRpcWorker(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("measurer", measurer);
                    }
                }, rpcWorkerCfg).toMap());
            }

            if (OpflowUtil.isComponentEnabled(subscriberCfg)) {
                String pubsubHandlerId = OpflowUUID.getBase64ID();
                subscriberCfg.put("pubsubHandlerId", pubsubHandlerId);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("pubsubHandlerId", pubsubHandlerId)
                        .text("Serverlet[${serverletId}] creates a new subscriber[${pubsubHandlerId}]")
                        .stringify());
                subscriber = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("measurer", measurer);
                    }
                }, subscriberCfg).toMap());
            }
            
            if (rpcWorker != null || subscriber != null) {
                instantiator = new Instantiator(rpcWorker, subscriber, OpflowObjectTree.buildMap(false)
                        .put(CONST.COMPONENT_ID, instanceId)
                        .toMap());
            }
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Serverlet[${serverletId}].new() end!")
                .stringify());
        
        measurer.updateComponentInstance("serverlet", instanceId, OpflowPromMeasurer.GaugeAction.INC);
    }
    
    public final void start() {
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Serverlet[${serverletId}].start()")
                .stringify());
        
        if (configurer != null && listenerMap.getConfigurer() != null) {
            configurer.subscribe(listenerMap.getConfigurer());
        }
        
        if (rpcWorker != null) {
            Map<String, OpflowRpcListener> rpcListeners = listenerMap.getRpcListeners();
            for(Map.Entry<String, OpflowRpcListener> entry:rpcListeners.entrySet()) {
                rpcWorker.process(entry.getKey(), entry.getValue());
            }
        }
        
        if (subscriber != null && listenerMap.getSubscriber() != null) {
            subscriber.subscribe(listenerMap.getSubscriber());
        }

        if (instantiator != null) {
            instantiator.process();
        }
        
        this.instantiateType(OpflowRpcCheckerWorker.class);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Serverlet[${serverletId}].start() has completed!")
                .stringify());
    }
    
    public void instantiateType(Class type) {
        instantiateType(type, null);
    }
    
    public void instantiateType(Class type, Object target) {
        if (instantiator != null) {
            instantiator.instantiateType(type, target);
        } else {
            throw new UnsupportedOperationException("instantiator is nulls");
        }
    }
    
    public void instantiateTypes(Class[] types) {
        instantiateTypes(Arrays.asList(types));
    }
    
    public void instantiateTypes(Collection<Class> types) {
        Set<Class> typeSet = new HashSet<>();
        typeSet.addAll(types);
        for (Class type : typeSet) {
            if (!Modifier.isAbstract(type.getModifiers())) {
                instantiateType(type);
            }
        }
    }
    
    @Override
    public final void close() {
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Serverlet[${serverletId}].close()")
                .stringify());
        
        if (configurer != null) configurer.close();
        if (rpcWorker != null) rpcWorker.close();
        if (subscriber != null) subscriber.close();

        OpflowUUID.release();

        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Serverlet[${serverletId}].close() has completed!")
                .stringify());
    }

    public static DescriptorBuilder getDescriptorBuilder() {
        return new DescriptorBuilder();
    }
    
    public static class DescriptorBuilder {
        private DescriptorBuilder() {}
        
        private ListenerDescriptor map = new ListenerDescriptor();
        
        public DescriptorBuilder setConfigurer(OpflowPubsubListener configurer) {
            map.configurer = configurer;
            return this;
        }
        
        public DescriptorBuilder setSubscriber(OpflowPubsubListener subscriber) {
            map.subscriber = subscriber;
            return this;
        }
        
        public DescriptorBuilder addRpcListener(String routineId, OpflowRpcListener listener) {
            map.rpcListeners.put(routineId, listener);
            return this;
        }
        
        public ListenerDescriptor build() {
            return map;
        }
    }
    
    public static class ListenerDescriptor {
        public static final ListenerDescriptor EMPTY = new ListenerDescriptor();
        private OpflowPubsubListener configurer;
        private Map<String, OpflowRpcListener> rpcListeners = new HashMap<>();
        private OpflowPubsubListener subscriber;
        
        private ListenerDescriptor() {}
        
        public OpflowPubsubListener getConfigurer() {
            return configurer;
        }

        public Map<String, OpflowRpcListener> getRpcListeners() {
            Map<String, OpflowRpcListener> cloned = new HashMap<>();
            cloned.putAll(rpcListeners);
            return cloned;
        }

        public OpflowPubsubListener getSubscriber() {
            return subscriber;
        }
    }
    
    public static class Instantiator {
        private static final Logger LOG = LoggerFactory.getLogger(Instantiator.class);
        private final OpflowLogTracer logTracer;
        private final OpflowRpcWorker rpcWorker;
        private final OpflowRpcListener rpcListener;
        private final OpflowPubsubHandler subscriber;
        private final OpflowPubsubListener subListener;
        private final Set<String> routineIds = new HashSet<>();
        private final Map<String, Method> methodRef = new HashMap<>();
        private final Map<String, Object> targetRef = new HashMap<>();
        private final Map<String, String> methodOfAlias = new HashMap<>();
        private boolean processing = false;
        
        public Instantiator(OpflowRpcWorker worker, OpflowPubsubHandler subscriber) throws OpflowBootstrapException {
            this(worker, subscriber, null);
        }
        
        public Instantiator(OpflowRpcWorker worker, OpflowPubsubHandler subscriber, Map<String, Object> options) throws OpflowBootstrapException {
            if (worker == null && subscriber == null) {
                throw new OpflowBootstrapException("Both of RpcWorker and subscriber must not be null");
            }
            options = OpflowUtil.ensureNotNull(options);
            final String instanceId = OpflowUtil.getOptionField(options, CONST.COMPONENT_ID, true);
            this.logTracer = OpflowLogTracer.ROOT.branch("instantiatorId", instanceId);
            this.rpcWorker = worker;
            this.rpcListener = new OpflowRpcListener() {
                @Override
                public Boolean processMessage(final OpflowMessage message, final OpflowRpcResponse response) throws IOException {
                    final Map<String, Object> headers = message.getInfo();
                    final String requestId = OpflowUtil.getRequestId(headers);
                    final String requestTime = OpflowUtil.getRequestTime(headers);
                    final String routineId = OpflowUtil.getRoutineId(headers);
                    final String methodId = methodOfAlias.getOrDefault(routineId, routineId);
                    final OpflowLogTracer reqTracer = logTracer.branch("requestTime", requestTime)
                            .branch("requestId", requestId, new OpflowLogTracer.OmitPingLogs(headers));
                    if (reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .put("methodId", methodId)
                            .text("Request[${requestId}][${requestTime}] - Serverlet[${instantiatorId}] receives a RPC call to the method[${methodId}]")
                            .stringify());
                    Method method = methodRef.get(methodId);
                    Object target = targetRef.get(methodId);
                    try {
                        Method origin = target.getClass().getMethod(method.getName(), method.getParameterTypes());
                        OpflowTargetRoutine routine = extractMethodInfo(origin);
                        if (routine != null && routine.enabled() == false) {
                            throw new UnsupportedOperationException("Method " + origin.toString() + " is disabled");
                        }
                        
                        String json = message.getBodyAsString();
                        if (reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                                .put("arguments", json)
                                .text("Request[${requestId}][${requestTime}] - Method arguments in json string")
                                .stringify());
                        Object[] args = OpflowJsonTool.toObjectArray(json, method.getParameterTypes());
                        
                        Object returnValue;
                        
                        String pingSignature = OpflowRpcCheckerWorker.getSendMethodName();
                        if (pingSignature.equals(routineId)) {
                            if (args.length > 0) {
                                OpflowRpcChecker.Ping p = (OpflowRpcChecker.Ping) args[0];
                                if (p.q != null) {
                                    String q = p.q;
                                    if (q.equals(getClassNameLabel(IllegalAccessException.class))) {
                                        throw new IllegalAccessException();
                                    }
                                    if (q.equals(getClassNameLabel(IllegalArgumentException.class))) {
                                        throw new IllegalArgumentException();
                                    }
                                    if (q.equals(getClassNameLabel(NoSuchMethodException.class))) {
                                        throw new NoSuchMethodException();
                                    }
                                    if (q.equals(getClassNameLabel(SecurityException.class))) {
                                        throw new SecurityException();
                                    }
                                    if (q.equals(getClassNameLabel(UnsupportedOperationException.class))) {
                                        throw new UnsupportedOperationException();
                                    }
                                    if (q.equals(getClassNameLabel(JsonSyntaxException.class))) {
                                        OpflowJsonTool.toObject("{opflow}", OpflowRpcChecker.Ping.class);
                                        throw new Exception();
                                    }
                                }
                            }
                            returnValue = new OpflowRpcChecker.Pong(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                                @Override
                                public void transform(Map<String, Object> opts) {
                                    OpflowEngine engine = rpcWorker.getEngine();
                                    opts.put(CONST.COMPONENT_ID, instanceId);
                                    opts.put("rpcWorker", OpflowObjectTree.buildMap()
                                            .put(CONST.COMPONENT_ID, rpcWorker.getIntanceId())
                                            .put("applicationId", engine.getApplicationId())
                                            .put("exchangeName", engine.getExchangeName())
                                            .put("routingKey", engine.getRoutingKey())
                                            .put("otherKeys", engine.getOtherKeys())
                                            .put("dispatchQueue", rpcWorker.getDispatchName())
                                            .put("handler", OpflowObjectTree.buildMap()
                                                    .put("requestId", requestId)
                                                    .put("requestTime", requestTime)
                                                    .put("applicationId", response.getApplicationId())
                                                    .put("replyToQueue", response.getReplyQueueName())
                                                    .put("consumerTag", response.getConsumerTag())
                                                    .toMap())
                                            .toMap());
                                }
                            }).toMap());
                        } else {
                            if (reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                    .put("targetName", target.getClass().getName())
                                    .text("Request[${requestId}][${requestTime}] - The method from target[${targetName}] is invoked")
                                    .stringify());
                            returnValue = method.invoke(target, args);
                        }
                        
                        String result = OpflowJsonTool.toString(returnValue);
                        if (reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                                .put("return", OpflowUtil.truncate(result))
                                .text("Request[${requestId}][${requestTime}] - Return the output of the method")
                                .stringify());
                        response.emitCompleted(result);
                        
                        if (reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .text("Request[${requestId}][${requestTime}] - Method call has completed")
                            .stringify());
                    } catch (JsonSyntaxException error) {
                        error.getStackTrace();
                        response.emitFailed(OpflowObjectTree.buildMap(false)
                                .put("exceptionClass", error.getClass().getName())
                                .put("exceptionPayload", OpflowJsonTool.toString(error))
                                .put("type", error.getClass().getName())
                                .put("message", error.getMessage())
                                .toString());
                        // throw error;
                    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException ex) {
                        LOG.error(null, ex); // not expected to happen
                        ex.getStackTrace();
                        response.emitFailed(OpflowObjectTree.buildMap(false)
                                .put("exceptionClass", ex.getClass().getName())
                                .put("exceptionPayload", OpflowJsonTool.toString(ex))
                                .put("type", ex.getClass().getName())
                                .put("message", ex.getMessage())
                                .toString());
                    } catch (InvocationTargetException ex) {
                        Throwable cause = (Exception) ex.getCause();
                        if (cause == null) {
                            cause = ex;
                        }
                        cause.getStackTrace();
                        response.emitFailed(OpflowObjectTree.buildMap(false)
                                .put("exceptionClass", cause.getClass().getName())
                                .put("exceptionPayload", OpflowJsonTool.toString(cause))
                                .put("type", cause.getClass().getName())
                                .put("message", cause.getMessage())
                                .toString());
                    } catch (UnsupportedOperationException ex) {
                        ex.getStackTrace();
                        response.emitFailed(OpflowObjectTree.buildMap(false)
                                .put("exceptionClass", ex.getClass().getName())
                                .put("exceptionPayload", OpflowJsonTool.toString(ex))
                                .put("type", ex.getClass().getName())
                                .put("message", ex.getMessage())
                                .toString());
                    } catch (Exception ex) {
                        response.emitFailed(OpflowObjectTree.buildMap(false)
                                .put("type", ex.getClass().getName())
                                .put("message", ex.getMessage())
                                .toString());
                    }
                    return null;
                }
            };
            this.subscriber = subscriber;
            this.subListener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowMessage message) throws IOException {
                    final Map<String, Object> headers = message.getInfo();
                    final String requestId = OpflowUtil.getRequestId(headers);
                    final String requestTime = OpflowUtil.getRequestTime(headers);
                    final String routineId = OpflowUtil.getRoutineId(headers);
                    final String methodId = methodOfAlias.getOrDefault(routineId, routineId);
                    final OpflowLogTracer reqTracer = logTracer.branch("requestTime", requestTime)
                            .branch("requestId", requestId, new OpflowLogTracer.OmitPingLogs(headers));
                    if (reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .put("routineId", routineId)
                            .put("methodId", methodId)
                            .text("Request[${requestId}][${requestTime}] - Serverlet[${instantiatorId}] receives an asynchronous method call [${routineId}]")
                            .stringify());
                    Method method = methodRef.get(methodId);
                    Object target = targetRef.get(methodId);
                    try {
                        Method origin = target.getClass().getMethod(method.getName(), method.getParameterTypes());
                        OpflowTargetRoutine routine = extractMethodInfo(origin);
                        if (routine != null && routine.enabled() == false) {
                            throw new UnsupportedOperationException("Method " + origin.toString() + " is disabled");
                        }
                        
                        String json = message.getBodyAsString();
                        if (reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                                .put("arguments", json)
                                .text("Request[${requestId}][${requestTime}] - Method arguments in json string")
                                .stringify());
                        Object[] args = OpflowJsonTool.toObjectArray(json, method.getParameterTypes());
                        
                        method.invoke(target, args);
                        
                        if (reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                .text("Request[${requestId}][${requestTime}] - Method call has completed")
                                .stringify());
                    } catch (JsonSyntaxException error) {
                        throw error;
                    } catch (IllegalAccessException | IllegalArgumentException | NoSuchMethodException | SecurityException | UnsupportedOperationException ex) {
                        throw new IOException(ex);
                    } catch (InvocationTargetException exception) {
                        Throwable catched = exception.getCause();
                        catched.getStackTrace();
                        throw new IOException(catched);
                    }
                }
            };
            if (Boolean.TRUE.equals(options.get("autorun"))) {
                process();
            }
        }
        
        public final void process() {
            if (!processing) {
                if (rpcWorker != null) {
                    rpcWorker.process(routineIds, rpcListener);
                }
                if (subscriber != null) {
                    subscriber.subscribe(subListener);
                }
                processing = true;
            }
        }
        
        public void instantiateType(Class type) {
            instantiateType(type, null);
        }
        
        public void instantiateType(Class type, Object target) {
            if (type == null) {
                throw new OpflowInterceptionException("The [type] parameter must not be null");
            }
            if (Modifier.isAbstract(type.getModifiers()) && target == null) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .text("Class should not be an abstract type")
                        .stringify());
                throw new OpflowInterceptionException("Class should not be an abstract type");
            }
            try {
                if (target == null) target = type.getDeclaredConstructor().newInstance();
                for (Method method : type.getDeclaredMethods()) {
                    String methodId = OpflowUtil.getMethodSignature(method);
                    OpflowTargetRoutine routine = extractMethodInfo(method);
                    if (routine != null && routine.alias() != null) {
                        String[] aliases = routine.alias();
                        for(String alias:aliases) {
                            if (methodOfAlias.containsKey(alias)) {
                                throw new OpflowInterceptionException("Alias[" + alias + "]/routineId[" + methodId + "]" + 
                                        " is conflicted with alias of routineId[" + methodOfAlias.get(alias) + "]");
                            }
                            methodOfAlias.put(alias, methodId);
                            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                                    .put("alias", alias)
                                    .put("routineId", methodId)
                                    .text("link alias to routineId")
                                    .stringify());
                        }
                    }
                }
                routineIds.addAll(methodOfAlias.keySet());
                List<Class<?>> clazzes = OpflowUtil.getAllAncestorTypes(type);
                for(Class clz: clazzes) {
                    Method[] methods = clz.getDeclaredMethods();
                    for (Method method : methods) {
                        String methodId = OpflowUtil.getMethodSignature(method);
                        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                                .put("rpcWorkerId", rpcWorker.getIntanceId())
                                .put("routineId", methodId)
                                .put("methodId", methodId)
                                .tags("attach-method-to-RpcWorker-listener")
                                .text("Attach the method[" + methodId + "] to the listener of RpcWorker[${rpcWorkerId}]")
                                .stringify());
                        if (!routineIds.add(methodId) && !method.equals(methodRef.get(methodId))) {
                            throw new OpflowInterceptionException("routineId[" + methodId + "] is conflicted");
                        }
                        methodRef.put(methodId, method);
                        targetRef.put(methodId, target);
                    }
                }
            } catch (InstantiationException except) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Could not instantiate the class")
                        .stringify());
                throw new OpflowInterceptionException("Could not instantiate the class", except);
            } catch (IllegalAccessException except) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Constructor is not accessible")
                        .stringify());
                throw new OpflowInterceptionException("Constructor is not accessible", except);
            } catch (SecurityException except) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Class loaders is not the same or denies access")
                        .stringify());
                throw new OpflowInterceptionException("Class loaders is not the same or denies access", except);
            } catch (Exception except) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Unknown exception")
                        .stringify());
                throw new OpflowInterceptionException("Unknown exception", except);
            }
            process();
        }
        
        private OpflowTargetRoutine extractMethodInfo(Method method) {
            if (method.isAnnotationPresent(OpflowTargetRoutine.class)) {
                Annotation annotation = method.getAnnotation(OpflowTargetRoutine.class);
                OpflowTargetRoutine routine = (OpflowTargetRoutine) annotation;
                return routine;
            }
            return null;
        }
    }
    
    public static class OpflowRpcCheckerWorker extends OpflowRpcChecker {
        @Override
        public Pong send(Ping info) throws Throwable {
            return new Pong();
        }
    }

    private static String getClassNameLabel(Class clazz) {
        return "throw-" + OpflowUtil.getClassSimpleName(clazz);
    }

    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance("serverlet", instanceId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
