package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.annotation.OpflowTargetRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInstantiationException;
import com.devebot.opflow.exception.OpflowJsonSyntaxException;
import com.devebot.opflow.exception.OpflowMethodNotFoundException;
import com.devebot.opflow.exception.OpflowTargetNotFoundException;
import com.devebot.opflow.supports.OpflowCollectionUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowSystemInfo;
import java.io.IOException;
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

    public final static List<String> SERVICE_BEAN_NAMES = Arrays.asList(new String[]{
        OpflowConstant.COMP_RPC_AMQP_WORKER,
        OpflowConstant.COMP_SUBSCRIBER
    });

    public final static List<String> SUPPORT_BEAN_NAMES = Arrays.asList(new String[]{
        OpflowConstant.COMP_DISCOVERY_CLIENT,
        OpflowConstant.COMP_PROM_EXPORTER,
        OpflowConstant.COMP_RPC_HTTP_WORKER,
    });

    public final static List<String> ALL_BEAN_NAMES = OpflowCollectionUtil.<String>mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);

    private final static Logger LOG = LoggerFactory.getLogger(OpflowServerlet.class);

    private final boolean strictMode;
    private final String componentId;
    private final String serviceName;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowConfig.Loader configLoader;
    private OpflowDiscoveryWorker discoveryWorker;

    private OpflowRpcAmqpWorker amqpWorker;
    private OpflowRpcHttpWorker httpWorker;
    private OpflowPubsubHandler subscriber;
    private Instantiator instantiator;

    private final ListenerDescriptor listenerMap;

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

        kwargs = OpflowObjectTree.ensureNonNull(kwargs);

        strictMode = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COMMON_STRICT, Boolean.FALSE);

        serviceName = getServiceName(kwargs);
        componentId = OpflowUtil.getStringField(kwargs, OpflowConstant.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("serverletId", componentId);

        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("Serverlet[${serverletId}][${instanceId}].new()")
                .stringify());
        }

        if (listeners == null) {
            throw new OpflowBootstrapException("Listener definitions must not be null");
        }
        listenerMap = listeners;

        measurer = OpflowPromMeasurer.getInstance(OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_PROM_EXPORTER));

        Map<String, Object> discoveryClientCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_DISCOVERY_CLIENT);
        
        if (OpflowUtil.isComponentExplicitEnabled(discoveryClientCfg)) {
            discoveryWorker = new OpflowDiscoveryWorker(componentId, serviceName, discoveryClientCfg);
        }
        
        Map<String, Object> amqpWorkerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_AMQP_WORKER, OpflowConstant.COMP_CFG_AMQP_WORKER);
        Map<String, Object> httpWorkerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_HTTP_WORKER);
        Map<String, Object> subscriberCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_SUBSCRIBER);

        HashSet<String> checkExchange = new HashSet<>();
        HashSet<String> checkQueue = new HashSet<>();
        HashSet<String> checkRecyclebin = new HashSet<>();

        if (OpflowUtil.isComponentEnabled(amqpWorkerCfg)) {
            if (!OpflowUtil.isAMQPEntrypointNull(amqpWorkerCfg, OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY)) {
                if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(amqpWorkerCfg, OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY))) {
                    throw new OpflowBootstrapException("Duplicated amqpWorker connection parameters (exchangeName-routingKey)");
                }
            }
            if (amqpWorkerCfg.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME) != null && !checkQueue.add(amqpWorkerCfg.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME).toString())) {
                throw new OpflowBootstrapException("amqpWorker[incomingQueueName] must not be duplicated");
            }
            if (amqpWorkerCfg.get(OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME) != null && !checkQueue.add(amqpWorkerCfg.get(OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME).toString())) {
                throw new OpflowBootstrapException("amqpWorker[responseQueueName] must not be duplicated");
            }
        }

        if (OpflowUtil.isComponentEnabled(subscriberCfg)) {
            if (!OpflowUtil.isAMQPEntrypointNull(subscriberCfg)) {
                if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(subscriberCfg))) {
                    throw new OpflowBootstrapException("Duplicated Subscriber connection parameters (exchangeName-routingKey)");
                }
            }
            if (subscriberCfg.get(OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME) != null && !checkQueue.add(subscriberCfg.get(OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME).toString())) {
                throw new OpflowBootstrapException("Subscriber[subscriberName] must not be duplicated");
            }
            if (subscriberCfg.get(OpflowConstant.OPFLOW_PUBSUB_TRASH_NAME) != null) {
                checkRecyclebin.add(subscriberCfg.get(OpflowConstant.OPFLOW_PUBSUB_TRASH_NAME).toString());
            }
        }

        checkRecyclebin.retainAll(checkQueue);
        if (!checkRecyclebin.isEmpty()) {
            if (logTracer.ready(LOG, Level.ERROR)) {
                LOG.error(logTracer
                    .text("duplicated_recyclebin_queue_name").toString());
            }
            throw new OpflowBootstrapException("Invalid recyclebinName (duplicated with some queueNames)");
        }

        try {
            if (OpflowUtil.isComponentEnabled(amqpWorkerCfg)) {
                String amqpWorkerId = OpflowUUID.getBase64ID();
                amqpWorkerCfg.put(OpflowConstant.COMPONENT_ID, amqpWorkerId);
                if (logTracer.ready(LOG, Level.INFO)) {
                    LOG.info(logTracer
                        .put("amqpWorkerId", amqpWorkerId)
                        .text("Serverlet[${serverletId}] creates a new amqpWorker[${amqpWorkerId}]")
                        .stringify());
                }
                amqpWorker = new OpflowRpcAmqpWorker(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(OpflowConstant.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    }
                }, amqpWorkerCfg).toMap());
            }
            
            if (OpflowUtil.isComponentEnabled(httpWorkerCfg)) {
                String httpWorkerId = OpflowUUID.getBase64ID();
                httpWorkerCfg.put(OpflowConstant.COMPONENT_ID, httpWorkerId);
                if (logTracer.ready(LOG, Level.INFO)) {
                    LOG.info(logTracer
                        .put("httpWorkerId", httpWorkerId)
                        .text("Serverlet[${serverletId}] creates a new httpWorker[${httpWorkerId}]")
                        .stringify());
                }
                httpWorker = new OpflowRpcHttpWorker(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(OpflowConstant.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    }
                }, httpWorkerCfg).toMap());
            }

            if (httpWorker != null && discoveryWorker != null) {
                discoveryWorker.setAddress(httpWorker.getHostname());
                discoveryWorker.setPort(httpWorker.getPort());
            }
            
            if (amqpWorker != null && httpWorker != null) {
                amqpWorker.setHttpAddress(httpWorker.getAddress());
            }
            
            if (OpflowUtil.isComponentEnabled(subscriberCfg)) {
                String pubsubHandlerId = OpflowUUID.getBase64ID();
                subscriberCfg.put("pubsubHandlerId", pubsubHandlerId);
                if (logTracer.ready(LOG, Level.INFO)) {
                    LOG.info(logTracer
                        .put("pubsubHandlerId", pubsubHandlerId)
                        .text("Serverlet[${serverletId}] creates a new subscriber[${pubsubHandlerId}]")
                        .stringify());
                }
                subscriber = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(OpflowConstant.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    }
                }, subscriberCfg).toMap());
            }
            
            if (amqpWorker != null || httpWorker != null || subscriber != null) {
                instantiator = new Instantiator(amqpWorker, httpWorker, subscriber, OpflowObjectTree.buildMap(false)
                    .put(OpflowConstant.COMPONENT_ID, componentId)
                    .toMap());
            }
        } catch (OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }

        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("Serverlet[${serverletId}][${instanceId}].new() end!")
                .stringify());
        }

        measurer.updateComponentInstance(OpflowConstant.COMP_SERVERLET, componentId, OpflowPromMeasurer.GaugeAction.INC);
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                close();
            }
        });
    }

    @Deprecated
    public final void start() {
        serve();
    }

    public final void serve() {
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("Serverlet[${serverletId}][${instanceId}].serve()")
                .stringify());
        }
        
        if (discoveryWorker != null) {
            discoveryWorker.serve();
        }
        
        if (amqpWorker != null) {
            Map<String, OpflowRpcAmqpWorker.Listener> rpcListeners = listenerMap.getRpcListeners();
            for (Map.Entry<String, OpflowRpcAmqpWorker.Listener> entry : rpcListeners.entrySet()) {
                amqpWorker.process(entry.getKey(), entry.getValue());
            }
        }
        
        if (httpWorker != null) {
            httpWorker.serve();
        }

        if (subscriber != null && listenerMap.getSubscriber() != null) {
            subscriber.subscribe(listenerMap.getSubscriber());
        }

        this.instantiateType(OpflowRpcCheckerWorker.class);
        
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                .put("pingSignature", OpflowRpcCheckerWorker.getPingSignature())
                .text("Serverlet[${serverletId}][${instanceId}].serve() uses the pingSignature: [${pingSignature}]")
                .stringify());

        if (instantiator != null) {
            instantiator.process();
        }

        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("Serverlet[${serverletId}][${instanceId}].serve() end!")
                .stringify());
        }
    }

    public void instantiateType(Class type) {
        instantiateType(type, null);
    }

    public void instantiateType(Class type, Object target) {
        if (instantiator != null) {
            instantiator.instantiateType(type, target);
        } else {
            throw new UnsupportedOperationException("instantiator is null");
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
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("Serverlet[${serverletId}][${instanceId}].close()")
                .stringify());
        }

        if (discoveryWorker != null) {
            discoveryWorker.close();
        }
        
        if (amqpWorker != null) {
            amqpWorker.close();
        }
        if (httpWorker != null) {
            httpWorker.close();
        }
        if (subscriber != null) {
            subscriber.close();
        }

        OpflowUUID.release();

        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("Serverlet[${serverletId}][${instanceId}].close() end!")
                .stringify());
        }
    }

    public static DescriptorBuilder getDescriptorBuilder() {
        return new DescriptorBuilder();
    }

    public static class DescriptorBuilder {

        private DescriptorBuilder() {
        }

        private ListenerDescriptor map = new ListenerDescriptor();

        public DescriptorBuilder setSubscriber(OpflowPubsubListener subscriber) {
            map.subscriber = subscriber;
            return this;
        }

        public DescriptorBuilder addRpcListener(String routineSignature, OpflowRpcAmqpWorker.Listener listener) {
            map.rpcListeners.put(routineSignature, listener);
            return this;
        }

        public ListenerDescriptor build() {
            return map;
        }
    }

    public static class ListenerDescriptor {

        public static final ListenerDescriptor EMPTY = new ListenerDescriptor();
        private Map<String, OpflowRpcAmqpWorker.Listener> rpcListeners = new HashMap<>();
        private OpflowPubsubListener subscriber;

        private ListenerDescriptor() {
        }

        public Map<String, OpflowRpcAmqpWorker.Listener> getRpcListeners() {
            Map<String, OpflowRpcAmqpWorker.Listener> cloned = new HashMap<>();
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
        private final OpflowRpcAmqpWorker amqpWorker;
        private final OpflowRpcAmqpWorker.Listener amqpListener;
        private final OpflowRpcHttpWorker httpWorker;
        private final OpflowRpcHttpWorker.Listener httpListener;
        private final OpflowPubsubHandler subscriber;
        private final OpflowPubsubListener subListener;
        private final Set<String> routineSignatures = new HashSet<>();
        private final Map<String, Method> methodRef = new HashMap<>();
        private final Map<String, Object> targetRef = new HashMap<>();
        private final Map<String, String> methodOfAlias = new HashMap<>();
        private volatile boolean processing = false;
        
        public Instantiator(OpflowRpcAmqpWorker amqpWorker, OpflowRpcHttpWorker httpWorker, OpflowPubsubHandler subscriber) throws OpflowBootstrapException {
            this(amqpWorker, httpWorker, subscriber, null);
        }

        public Instantiator(OpflowRpcAmqpWorker amqpWorker, OpflowRpcHttpWorker httpWorker, OpflowPubsubHandler subscriber, Map<String, Object> options) throws OpflowBootstrapException {
            if (amqpWorker == null && subscriber == null) {
                throw new OpflowBootstrapException("Both of amqpWorker and subscriber must not be null");
            }
            options = OpflowObjectTree.ensureNonNull(options);
            final String componentId = OpflowUtil.getStringField(options, OpflowConstant.COMPONENT_ID, true);
            this.logTracer = OpflowLogTracer.ROOT.branch("instantiatorId", componentId);
            
            this.amqpWorker = amqpWorker;
            this.amqpListener = new OpflowRpcAmqpWorker.Listener() {
                @Override
                public Boolean processMessage(final OpflowEngine.Message message, final OpflowRpcAmqpResponse response) throws IOException {
                    final Map<String, Object> headers = message.getHeaders();
                    final String routineId = response.getRoutineId();
                    final String routineTimestamp = response.getRoutineTimestamp();
                    final String routineScope = response.getRoutineScope();
                    final String routineSignature = response.getRoutineSignature();
                    final String body = message.getBodyAsString();
                    
                    Map<String, String> extra = OpflowObjectTree.<String>buildMap()
                        .put("replyToQueue", response.getReplyQueueName())
                        .put("consumerTag", response.getConsumerTag())
                        .toMap();
                    
                    RoutineOutput output = invokeRoutine(OpflowConstant.Protocol.AMQP, body, routineSignature, routineScope, routineTimestamp, routineId, componentId, extra);
                    output.fill(response);
                    
                    return null;
                }
            };
            
            this.httpWorker = httpWorker;
            this.httpListener = new OpflowRpcHttpWorker.Listener() {
                @Override
                public OpflowRpcHttpWorker.Output processMessage(String body, String routineSignature, String routineScope, String routineTimestamp, String routineId, Map<String, String> extra) {
                    return invokeRoutine(OpflowConstant.Protocol.HTTP, body, routineSignature, routineScope, routineTimestamp, routineId, componentId, extra).export();
                }
            };
            
            this.subscriber = subscriber;
            this.subListener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowEngine.Message message) throws IOException {
                    final Map<String, Object> headers = message.getHeaders();
                    final String routineId = OpflowUtil.getRoutineId(headers);
                    final String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);
                    final String routineSignature = OpflowUtil.getRoutineSignature(headers);
                    final String methodSignature = methodOfAlias.getOrDefault(routineSignature, routineSignature);
                    final OpflowLogTracer reqTracer = logTracer.branch(OpflowConstant.REQUEST_TIME, routineTimestamp)
                        .branch(OpflowConstant.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(headers));
                    if (reqTracer.ready(LOG, Level.INFO)) {
                        LOG.info(reqTracer
                            .put("routineSignature", routineSignature)
                            .put("methodSignature", methodSignature)
                            .text("Request[${requestId}][${requestTime}] - Serverlet[${instantiatorId}] receives an asynchronous routine call to method[${methodSignature}]")
                            .stringify());
                    }
                    Method method = methodRef.get(methodSignature);
                    Object target = targetRef.get(methodSignature);
                    assertMethodNotNull(methodSignature, method, target, reqTracer);
                    try {
                        Method origin = target.getClass().getMethod(method.getName(), method.getParameterTypes());
                        OpflowTargetRoutine routine = OpflowUtil.extractMethodAnnotation(origin, OpflowTargetRoutine.class);
                        if (routine != null && routine.enabled() == false) {
                            throw new UnsupportedOperationException("Method " + origin.toString() + " is disabled");
                        }

                        String json = message.getBodyAsString();
                        if (reqTracer.ready(LOG, Level.TRACE)) {
                            LOG.trace(reqTracer
                                .put("arguments", json)
                                .text("Request[${requestId}][${requestTime}] - Method arguments in json string")
                                .stringify());
                        }
                        Object[] args = OpflowJsonTool.toObjectArray(json, method.getParameterTypes());

                        method.invoke(target, args);

                        if (reqTracer.ready(LOG, Level.INFO)) {
                            LOG.info(reqTracer
                                .text("Request[${requestId}][${requestTime}] - Method call has completed")
                                .stringify());
                        }
                    } catch (OpflowJsonSyntaxException error) {
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
            
            if (Boolean.TRUE.equals(options.get(OpflowConstant.OPFLOW_COMMON_AUTORUN))) {
                process();
            }
        }
        
        private RoutineOutput invokeRoutine(
            final OpflowConstant.Protocol protocol,
            final String body,
            final String routineSignature,
            final String routineScope,
            final String routineTimestamp,
            final String routineId,
            final String componentId,
            final Map<String, String> extra
        ) {
            RoutineOutput output = null;
            final String methodSignature = methodOfAlias.getOrDefault(routineSignature, routineSignature);
            final OpflowLogTracer reqTracer = logTracer.branch(OpflowConstant.REQUEST_TIME, routineTimestamp)
                .branch(OpflowConstant.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(routineScope));
            if (reqTracer.ready(LOG, Level.INFO)) {
                LOG.info(reqTracer
                    .put("methodSignature", methodSignature)
                    .text("Request[${requestId}][${requestTime}][x-serverlet-rpc-received]"
                        + " - Serverlet[${instantiatorId}][${instanceId}] receives a RPC call to the routine[${methodSignature}]")
                    .stringify());
            }
            Method method = methodRef.get(methodSignature);
            Object target = targetRef.get(methodSignature);
            assertMethodNotNull(methodSignature, method, target, reqTracer);
            try {
                Method origin = target.getClass().getMethod(method.getName(), method.getParameterTypes());
                OpflowTargetRoutine routine = OpflowUtil.extractMethodAnnotation(origin, OpflowTargetRoutine.class);;
                if (routine != null && routine.enabled() == false) {
                    throw new UnsupportedOperationException("Method " + origin.toString() + " is disabled");
                }

                if (reqTracer.ready(LOG, Level.TRACE)) {
                    LOG.trace(reqTracer
                        .put("arguments", body)
                        .text("Request[${requestId}][${requestTime}] - Method arguments in json string")
                        .stringify());
                }
                Object[] args = OpflowJsonTool.toObjectArray(body, method.getParameterTypes());
                
                Object returnValue;
                
                String pingSignature = OpflowRpcCheckerWorker.getSendMethodName();
                if (reqTracer.ready(LOG, Level.TRACE)) {
                    LOG.trace(reqTracer
                        .put("routineSignature", routineSignature)
                        .put("pingSignature", pingSignature)
                        .text("Request[${requestId}][${requestTime}] - compares the routine[${routineSignature}] with ping[${pingSignature}]")
                        .stringify());
                }
                if (pingSignature.equals(routineSignature)) {
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
                            if (q.equals(getClassNameLabel(OpflowJsonSyntaxException.class))) {
                                OpflowJsonTool.toObject("{opflow}", OpflowRpcChecker.Ping.class);
                                throw new Exception();
                            }
                        }
                    }
                    returnValue = new OpflowRpcChecker.Pong(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opts) {
                            Map<String, Object> requestInfo = OpflowObjectTree.buildMap()
                                    .put(OpflowConstant.ROUTINE_ID, routineId)
                                    .put(OpflowConstant.ROUTINE_TIMESTAMP, routineTimestamp)
                                    .add(extra)
                                    .toMap();
                            OpflowEngine engine = amqpWorker.getEngine();
                            opts.put(OpflowConstant.COMPONENT_ID, componentId);
                            opts.put(OpflowConstant.COMP_RPC_AMQP_WORKER, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.COMPONENT_ID, amqpWorker.getComponentId())
                                .put(OpflowConstant.OPFLOW_COMMON_APP_ID, engine.getApplicationId())
                                .put(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME, amqpWorker.getIncomingQueueName())
                                .put("request", requestInfo, protocol == OpflowConstant.Protocol.AMQP)
                                .toMap());
                            opts.put(OpflowConstant.COMP_RPC_HTTP_WORKER, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.COMPONENT_ID, httpWorker.getComponentId())
                                .put("request", requestInfo, protocol == OpflowConstant.Protocol.HTTP)
                                .toMap());
                            opts.put(OpflowConstant.INFO_SECTION_SOURCE_CODE, OpflowObjectTree.buildMap()
                                .put("server", getGitInfo())
                                .put(OpflowConstant.FRAMEWORK_ID, OpflowSystemInfo.getGitInfo())
                                .toMap());
                        }
                    }).toMap());
                } else {
                    if (reqTracer.ready(LOG, Level.INFO)) {
                        LOG.info(reqTracer
                            .put("targetName", target.getClass().getName())
                            .text("Request[${requestId}][${requestTime}][x-serverlet-rpc-processing] - The method from target[${targetName}] is invoked")
                            .stringify());
                    }
                    returnValue = method.invoke(target, args);
                }

                String result = OpflowJsonTool.toString(returnValue);
                if (reqTracer.ready(LOG, Level.TRACE)) {
                    LOG.trace(reqTracer
                        .put("return", OpflowUtil.truncate(result))
                        .text("Request[${requestId}][${requestTime}] - Return the output of the method")
                        .stringify());
                }
                output = RoutineOutput.asSuccess(result);

                if (reqTracer.ready(LOG, Level.INFO)) {
                    LOG.info(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-serverlet-rpc-completed] - Method call has completed")
                        .stringify());
                }
            } catch (OpflowJsonSyntaxException error) {
                error.getStackTrace();
                output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                    .put("exceptionClass", error.getClass().getName())
                    .put("exceptionPayload", OpflowJsonTool.toString(error))
                    .put("type", error.getClass().getName())
                    .put("message", error.getMessage())
                    .toString());
                // throw error;
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException ex) {
                LOG.error(null, ex); // not expected to happen
                ex.getStackTrace();
                output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
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
                output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                    .put("exceptionClass", cause.getClass().getName())
                    .put("exceptionPayload", OpflowJsonTool.toString(cause))
                    .put("type", cause.getClass().getName())
                    .put("message", cause.getMessage())
                    .toString());
            } catch (UnsupportedOperationException ex) {
                ex.getStackTrace();
                output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                    .put("exceptionClass", ex.getClass().getName())
                    .put("exceptionPayload", OpflowJsonTool.toString(ex))
                    .put("type", ex.getClass().getName())
                    .put("message", ex.getMessage())
                    .toString());
            } catch (Exception ex) {
                output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                    .put("type", ex.getClass().getName())
                    .put("message", ex.getMessage())
                    .toString());
            }
            return output;
        }
        
        private static class RoutineOutput {
            private boolean failed;
            private String value;
            private String error;
            
            public static RoutineOutput asSuccess(String value) {
                RoutineOutput that = new RoutineOutput();
                that.failed = false;
                that.value = value;
                return that;
            }
            
            public static RoutineOutput asFailure(String error) {
                RoutineOutput that = new RoutineOutput();
                that.failed = true;
                that.error = error;
                return that;
            }
            
            public void fill(OpflowRpcAmqpResponse response) {
                if (failed) {
                    response.emitFailed(error);
                } else {
                    response.emitCompleted(value);
                }
            }
            
            public OpflowRpcHttpWorker.Output export() {
                if (failed) {
                    return new OpflowRpcHttpWorker.Output(false, error);
                } else {
                    return new OpflowRpcHttpWorker.Output(true, value);
                }
            }
        }
        
        public final synchronized void process() {
            if (!processing) {
                if (amqpWorker != null) {
                    amqpWorker.process(routineSignatures, amqpListener);
                }
                if (httpWorker != null) {
                    httpWorker.process(routineSignatures, httpListener);
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
                throw new OpflowInstantiationException("The [type] parameter must not be null");
            }
            if (Modifier.isAbstract(type.getModifiers()) && target == null) {
                if (logTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(logTracer
                        .text("Class should not be an abstract type")
                        .stringify());
                }
                throw new OpflowInstantiationException("Class should not be an abstract type");
            }
            try {
                if (target == null) {
                    target = type.getDeclaredConstructor().newInstance();
                }
                for (Method method : type.getDeclaredMethods()) {
                    String methodSignature = OpflowUtil.getMethodSignature(method);
                    OpflowTargetRoutine routine = OpflowUtil.extractMethodAnnotation(method, OpflowTargetRoutine.class);
                    if (routine != null && routine.alias() != null) {
                        String[] aliases = routine.alias();
                        if (logTracer.ready(LOG, Level.DEBUG)) {
                            LOG.debug(logTracer
                                .put("methodSignature", methodSignature)
                                .put("numberOfAliases", aliases.length)
                                .text("Serverlet[${instantiatorId}].instantiateType() - method[${methodSignature}] has ${numberOfAliases} alias(es)")
                                .stringify());
                        }
                        for (String alias : aliases) {
                            if (methodOfAlias.containsKey(alias)) {
                                throw new OpflowInstantiationException("Alias[" + alias + "]/methodSignature[" + methodSignature + "]"
                                    + " is conflicted with alias of routineSignature[" + methodOfAlias.get(alias) + "]");
                            }
                            methodOfAlias.put(alias, methodSignature);
                            if (logTracer.ready(LOG, Level.DEBUG)) {
                                LOG.debug(logTracer
                                    .put("alias", alias)
                                    .put("methodSignature", methodSignature)
                                    .text("Serverlet[${instantiatorId}].instantiateType() - link the alias[${alias}] to the methodSignature[${methodSignature}]")
                                    .stringify());
                            }
                        }
                    }
                }
                routineSignatures.addAll(methodOfAlias.keySet());
                List<Class<?>> clazzes = OpflowUtil.getAllAncestorTypes(type);
                for (Class clz : clazzes) {
                    Method[] methods = clz.getDeclaredMethods();
                    for (Method method : methods) {
                        String methodSignature = OpflowUtil.getMethodSignature(method);
                        if (logTracer.ready(LOG, Level.TRACE)) {
                            LOG.trace(logTracer
                                .put("amqpWorkerId", amqpWorker.getComponentId())
                                .put("methodSignature", methodSignature)
                                .tags("attach-method-to-RpcWorker-listener")
                                .text("Attach the method[" + methodSignature + "] to the listener of amqpWorker[${amqpWorkerId}]")
                                .stringify());
                        }
                        if (!routineSignatures.add(methodSignature) && !method.equals(methodRef.get(methodSignature))) {
                            throw new OpflowInstantiationException("methodSignature[" + methodSignature + "] is conflicted");
                        }
                        methodRef.put(methodSignature, method);
                        targetRef.put(methodSignature, target);
                    }
                }
            } catch (InstantiationException except) {
                if (logTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Could not instantiate the class")
                        .stringify());
                }
                throw new OpflowInstantiationException("Could not instantiate the class", except);
            } catch (IllegalAccessException except) {
                if (logTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Constructor is not accessible")
                        .stringify());
                }
                throw new OpflowInstantiationException("Constructor is not accessible", except);
            } catch (SecurityException except) {
                if (logTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Class loaders is not the same or denies access")
                        .stringify());
                }
                throw new OpflowInstantiationException("Class loaders is not the same or denies access", except);
            } catch (Exception except) {
                if (logTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(logTracer
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .text("Unknown exception")
                        .stringify());
                }
                throw new OpflowInstantiationException("Unknown exception", except);
            }
            process();
        }

        private void assertMethodNotNull(String methodSignature, Method method, Object target, OpflowLogTracer reqTracer) {
            if (method == null) {
                if (reqTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(reqTracer
                        .put("methodSignature", methodSignature)
                        .text("Request[${requestId}][${requestTime}] - method[${methodSignature}] not found")
                        .stringify());
                }
                throw new OpflowMethodNotFoundException();
            }
            if (target == null) {
                if (reqTracer.ready(LOG, Level.ERROR)) {
                    LOG.error(reqTracer
                        .put("methodSignature", methodSignature)
                        .text("Request[${requestId}][${requestTime}] - target[${methodSignature}] not found")
                        .stringify());
                }
                throw new OpflowTargetNotFoundException();
            }
        }
    }

    public static class OpflowRpcCheckerWorker extends OpflowRpcChecker {

        @Override
        @OpflowTargetRoutine(alias = OpflowConstant.OPFLOW_ROUTINE_PINGPONG_ALIAS)
        public Pong send(Ping info) throws Throwable {
            return new Pong();
        }
    }

    private static String getClassNameLabel(Class clazz) {
        return "throw-" + OpflowUtil.getClassSimpleName(clazz);
    }

    private static String getServiceName(Map<String, Object> kwargs) {
        // load the serviceName from the configuration file
        String serviceName = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COMMON_SERVICE_NAME);
        if (serviceName != null) {
            return serviceName;
        }
        // use the project name as the serviceName
        // TODO: ...
        // use the appId with the suffix [worker]
        // TODO: ...
        // use the git commit-id
        serviceName = OpflowUtil.getStringField(getGitInfo(), "git.commit.id");
        return serviceName;
    }
    
    private static Map<String, Object> getGitInfo() {
        return OpflowSystemInfo.getGitInfo("META-INF/scm/service-worker/git-info.json");
    }
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_SERVERLET, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
