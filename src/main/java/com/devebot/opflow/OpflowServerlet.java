package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowServerlet {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowServerlet.class);
    private final OpflowLogTracer logTracer;
    
    private OpflowPubsubHandler configurer;
    private OpflowRpcWorker rpcWorker;
    private OpflowPubsubHandler subscriber;
    private Instantiator instantiator;
    
    private final ListenerMap listenerMap;
    
    private final Map<String, Object> kwargs;
    
    public OpflowServerlet(ListenerMap listeners, Map<String, Object> kwargs) throws OpflowBootstrapException {
        this.kwargs = OpflowUtil.ensureNotNull(kwargs);

        logTracer = OpflowLogTracer.ROOT.branch("serverletId", OpflowUtil.getOptionField(this.kwargs, "serverletId", true));
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Serverlet.new()")
                .toString());
        
        if (listeners == null) {
            throw new OpflowBootstrapException("Listener definitions must not be null");
        }
        listenerMap = listeners;
        
        Map<String, Object> configurerCfg = (Map<String, Object>)this.kwargs.get("configurer");
        Map<String, Object> rpcWorkerCfg = (Map<String, Object>)this.kwargs.get("rpcWorker");
        Map<String, Object> subscriberCfg = (Map<String, Object>)this.kwargs.get("subscriber");
        
        HashSet<String> checkExchange = new HashSet<String>();
        HashSet<String> checkQueue = new HashSet<String>();
        HashSet<String> checkRecyclebin = new HashSet<String>();
        
        if (configurerCfg != null && !Boolean.FALSE.equals(configurerCfg.get("enabled"))) {
            if (configurerCfg.get("exchangeName") == null || configurerCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            } 
            if (!checkExchange.add(configurerCfg.get("exchangeName").toString() + configurerCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters");
            }
            if (configurerCfg.get("subscriberName") != null && !checkQueue.add(configurerCfg.get("subscriberName").toString())) {
                throw new OpflowBootstrapException("Configurer[subscriberName] must not be duplicated");
            }
            if (configurerCfg.get("recyclebinName") != null) checkRecyclebin.add(configurerCfg.get("recyclebinName").toString());
        }

        if (rpcWorkerCfg != null && !Boolean.FALSE.equals(rpcWorkerCfg.get("enabled"))) {
            if (rpcWorkerCfg.get("exchangeName") == null || rpcWorkerCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid RpcWorker connection parameters");
            }
            if (!checkExchange.add(rpcWorkerCfg.get("exchangeName").toString() + rpcWorkerCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated RpcWorker connection parameters");
            }
            if (rpcWorkerCfg.get("operatorName") != null && !checkQueue.add(rpcWorkerCfg.get("operatorName").toString())) {
                throw new OpflowBootstrapException("RpcWorker[operatorName] must not be duplicated");
            }
            if (rpcWorkerCfg.get("responseName") != null && !checkQueue.add(rpcWorkerCfg.get("responseName").toString())) {
                throw new OpflowBootstrapException("RpcWorker[responseName] must not be duplicated");
            }
        }
        
        if (subscriberCfg != null && !Boolean.FALSE.equals(subscriberCfg.get("enabled"))) {
            if (subscriberCfg.get("exchangeName") == null || subscriberCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid Subscriber connection parameters");
            }
            if (!checkExchange.add(subscriberCfg.get("exchangeName").toString() + subscriberCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated Subscriber connection parameters");
            }
            if (subscriberCfg.get("subscriberName") != null && !checkQueue.add(subscriberCfg.get("subscriberName").toString())) {
                throw new OpflowBootstrapException("Subscriber[subscriberName] must not be duplicated");
            }
            if (subscriberCfg.get("recyclebinName") != null) checkRecyclebin.add(subscriberCfg.get("recyclebinName").toString());
        }
        
        checkRecyclebin.retainAll(checkQueue);
        if (!checkRecyclebin.isEmpty()) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer
                .put("message", "duplicated_recyclebin_queue_name").toString());
            throw new OpflowBootstrapException("Invalid recyclebinName (duplicated with some queueNames)");
        }
        
        try {
            if (configurerCfg != null && !Boolean.FALSE.equals(configurerCfg.get("enabled"))) {
                String pubsubHandlerId = OpflowUtil.getLogID();
                configurerCfg.put("pubsubHandlerId", pubsubHandlerId);
                if (LOG.isInfoEnabled()) LOG.info(logTracer
                        .put("pubsubHandlerId", pubsubHandlerId)
                        .put("message", "Serverlet creates a new configurer")
                        .stringify(true));
                configurer = new OpflowPubsubHandler(configurerCfg);
            }

            if (rpcWorkerCfg != null && !Boolean.FALSE.equals(rpcWorkerCfg.get("enabled"))) {
                String rpcWorkerId = OpflowUtil.getLogID();
                rpcWorkerCfg.put("rpcWorkerId", rpcWorkerId);
                if (LOG.isInfoEnabled()) LOG.info(logTracer
                        .put("rpcWorkerId", rpcWorkerId)
                        .put("message", "Serverlet creates a new rpcWorker")
                        .stringify(true));
                rpcWorker = new OpflowRpcWorker(rpcWorkerCfg);
                instantiator = new Instantiator(rpcWorker, OpflowUtil.buildMap()
                        .put("instantiatorId", rpcWorkerId).toMap());
            }

            if (subscriberCfg != null && !Boolean.FALSE.equals(subscriberCfg.get("enabled"))) {
                String pubsubHandlerId = OpflowUtil.getLogID();
                subscriberCfg.put("pubsubHandlerId", pubsubHandlerId);
                if (LOG.isInfoEnabled()) LOG.info(logTracer
                        .put("pubsubHandlerId", pubsubHandlerId)
                        .put("message", "Serverlet creates a new subscriber")
                        .stringify(true));
                subscriber = new OpflowPubsubHandler(subscriberCfg);
            }
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Serverlet.new() end!")
                .toString());
    }
    
    public final void start() {
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Serverlet start()")
                .toString());
        
        if (configurer != null) {
            configurer.subscribe(listenerMap.getConfigurer());
        }
        if (rpcWorker != null) {
            Map<String, OpflowRpcListener> rpcListeners = listenerMap.getRpcListeners();
            for(Map.Entry<String, OpflowRpcListener> entry:rpcListeners.entrySet()) {
                rpcWorker.process(entry.getKey(), entry.getValue());
            }
        }
        if (instantiator != null) {
            instantiator.process();
        }
        if (subscriber != null) {
            subscriber.subscribe(listenerMap.getSubscriber());
        }
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Serverlet start() has done!")
                .toString());
    }
    
    public void instantiateType(Class type) {
        if (instantiator != null) {
            instantiator.instantiateType(type);
        } else {
            throw new UnsupportedOperationException("instantiator is nulls");
        }
    }
    
    public final void close() {
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Serverlet stop()")
                .toString());
        
        if (configurer != null) configurer.close();
        if (rpcWorker != null) rpcWorker.close();
        if (subscriber != null) subscriber.close();
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Serverlet stop() has done!")
                .toString());
    }
    
    public static ListenerMapBuilder getListenerBuilder() {
        return new ListenerMapBuilder();
    }
    
    public static class ListenerMapBuilder {
        private ListenerMapBuilder() {}
        
        private ListenerMap map = new ListenerMap();
        
        public ListenerMapBuilder setConfigurer(OpflowPubsubListener configurer) {
            map.configurer = configurer;
            return this;
        }
        
        public ListenerMapBuilder setSubscriber(OpflowPubsubListener subscriber) {
            map.subscriber = subscriber;
            return this;
        }
        
        public ListenerMapBuilder addRpcListener(String routineId, OpflowRpcListener listener) {
            map.rpcListeners.put(routineId, listener);
            return this;
        }
        
        public ListenerMap build() {
            return map;
        }
    }
    
    public static class ListenerMap {
        private OpflowPubsubListener configurer;
        private Map<String, OpflowRpcListener> rpcListeners = new HashMap<String, OpflowRpcListener>();
        private OpflowPubsubListener subscriber;
        
        private ListenerMap() {}
        
        public OpflowPubsubListener getConfigurer() {
            return configurer;
        }

        public Map<String, OpflowRpcListener> getRpcListeners() {
            Map<String, OpflowRpcListener> cloned = new HashMap<String, OpflowRpcListener>();
            cloned.putAll(rpcListeners);
            return cloned;
        }

        public OpflowPubsubListener getSubscriber() {
            return subscriber;
        }
    }
    
    public static class Instantiator {
        private static final Logger LOG = LoggerFactory.getLogger(Instantiator.class);
        private static final Object[] EMPTY_ARGS = new Object[0];
        private final OpflowLogTracer logTracer;
        private final OpflowRpcWorker rpcWorker;
        private final OpflowRpcListener listener;
        private final Set<String> routineIds = new HashSet<String>();
        private final Map<String, Method> methodRef = new HashMap<String, Method>();
        private final Map<String, Object> targetRef = new HashMap<String, Object>();
        private boolean processing = false;
        
        public Instantiator(OpflowRpcWorker worker) throws OpflowBootstrapException {
            this(worker, null);
        }
        
        public Instantiator(OpflowRpcWorker worker, Map<String, Object> options) throws OpflowBootstrapException {
            if (worker == null) {
                throw new OpflowBootstrapException("RpcWorker should not be null");
            }
            options = OpflowUtil.ensureNotNull(options);
            logTracer = OpflowLogTracer.ROOT.branch("instantiatorId", options.getOrDefault("instantiatorId", OpflowUtil.getLogID()));
            rpcWorker = worker;
            listener = new OpflowRpcListener() {
                @Override
                public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                    String requestId = OpflowUtil.getRequestId(message.getInfo());
                    OpflowLogTracer listenerTrail = logTracer.branch("requestId", requestId);
                    String routineId = OpflowUtil.getRoutineId(message.getInfo());
                    if (LOG.isInfoEnabled()) LOG.info(listenerTrail
                            .put("routineId", routineId)
                            .put("message", "Receives new method call")
                            .stringify(true));
                    String json = message.getBodyAsString();
                    if (LOG.isTraceEnabled()) LOG.trace(listenerTrail
                            .put("arguments", json)
                            .put("message", "Method arguments in json string")
                            .stringify(true));
                    Object[] args = EMPTY_ARGS;
                    try {
                        args = OpflowUtil.jsonStringToArray(json, methodRef.get(routineId).getParameterTypes());
                    } catch (JsonSyntaxException error) {
                        response.emitFailed(OpflowUtil.buildMap()
                                .put("errorType", error.getClass().getName())
                                .put("errorMessage", error.getMessage())
                                .toString());
                        throw error;
                    }
                    try {
                        Object returnValue = methodRef.get(routineId).invoke(targetRef.get(routineId), args);
                        String result = OpflowUtil.jsonObjectToString(returnValue);
                        if (LOG.isTraceEnabled()) LOG.trace(listenerTrail
                                .put("return", OpflowUtil.truncate(result))
                                .put("message", "Return value of method")
                                .stringify(true));
                        response.emitCompleted(result);
                        if (LOG.isInfoEnabled()) LOG.info(listenerTrail
                            .put("message", "Method call has completed")
                            .stringify());
                    } catch (IllegalAccessException ex) {
                        LOG.error(null, ex);
                        response.emitFailed(OpflowUtil.buildMap()
                                .put("errorType", ex.getClass().getName())
                                .put("errorMessage", ex.getMessage())
                                .toString());
                    } catch (IllegalArgumentException ex) {
                        LOG.error(null, ex);
                        response.emitFailed(OpflowUtil.buildMap()
                                .put("errorType", ex.getClass().getName())
                                .put("errorMessage", ex.getMessage())
                                .toString());
                    } catch (InvocationTargetException ex) {
                        LOG.error(null, ex);
                        response.emitFailed(OpflowUtil.buildMap()
                                .put("errorType", ex.getClass().getName())
                                .put("errorMessage", ex.getMessage())
                                .toString());
                    } catch (Exception ex) {
                        response.emitFailed(OpflowUtil.buildMap()
                                .put("errorType", ex.getClass().getName())
                                .put("errorMessage", ex.getMessage())
                                .toString());
                    }
                    return null;
                }
            };
            if (Boolean.TRUE.equals(options.get("autorun"))) {
                process();
            }
        }
        
        public final void process() {
            if (!processing) {
                rpcWorker.process(routineIds, listener);
                processing = true;
            }
        }
        
        public void instantiateType(Class type) {
            if (Modifier.isAbstract(type.getModifiers())) {
                if (LOG.isErrorEnabled()) LOG.error(logTracer
                        .put("message", "Class should not be an abstract type")
                        .stringify(true));
                throw new OpflowInterceptionException("Class should not be an abstract type");
            }
            try {
                List<Class> clazzes = new LinkedList<Class>();
                clazzes.add(type);
                Class[] interfaces = type.getInterfaces();
                clazzes.addAll(Arrays.asList(interfaces));

                Object target = type.newInstance();
                for(Class clz: clazzes) {
                    Method[] methods = clz.getDeclaredMethods();
                    for (Method method : methods) {
                        String routineId = method.toString();
                        if (LOG.isTraceEnabled()) LOG.trace(logTracer
                                .put("routineId", routineId)
                                .put("message", "Attach method to RpcWorker listener")
                                .stringify(true));
                        routineIds.add(routineId);
                        methodRef.put(routineId, method);
                        targetRef.put(routineId, target);
                    }
                }
            } catch (InstantiationException except) {
                if (LOG.isErrorEnabled()) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .put("message", "Could not instantiate the class")
                        .stringify(true));
                throw new OpflowInterceptionException("Could not instantiate the class", except);
            } catch (IllegalAccessException except) {
                if (LOG.isErrorEnabled()) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .put("message", "Constructor is not accessible")
                        .stringify(true));
                throw new OpflowInterceptionException("Constructor is not accessible", except);
            } catch (SecurityException except) {
                if (LOG.isErrorEnabled()) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .put("message", "Class loaders is not the same or denies access")
                        .stringify(true));
                throw new OpflowInterceptionException("Class loaders is not the same or denies access", except);
            } catch (Exception except) {
                if (LOG.isErrorEnabled()) LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .put("message", "Unknown exception")
                        .stringify(true));
                throw new OpflowInterceptionException("Unknown exception", except);
            }
            process();
        }
    }
}
