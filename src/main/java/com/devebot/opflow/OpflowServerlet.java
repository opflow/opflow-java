package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.annotation.OpflowTargetRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowCollectionUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
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
        
        Map<String, Object> amqpWorkerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_AMQP_WORKER);
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

    public static class Instantiator extends OpflowMethodInvoker {
        public Instantiator(OpflowRpcAmqpWorker amqpWorker, OpflowRpcHttpWorker httpWorker, OpflowPubsubHandler subscriber) throws OpflowBootstrapException {
            super(amqpWorker, httpWorker, subscriber);
        }

        public Instantiator(OpflowRpcAmqpWorker amqpWorker, OpflowRpcHttpWorker httpWorker, OpflowPubsubHandler subscriber, Map<String, Object> options) throws OpflowBootstrapException {
            super(amqpWorker, httpWorker, subscriber, options);
        }
    }

    public static class OpflowRpcCheckerWorker extends OpflowRpcChecker {

        @Override
        @OpflowTargetRoutine(alias = OpflowConstant.OPFLOW_ROUTINE_PINGPONG_ALIAS)
        public Pong send(Ping info) throws Throwable {
            return new Pong();
        }
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
        serviceName = OpflowUtil.getStringField(OpflowUtil.getGitInfoFromWorker(), "git.commit.id");
        return serviceName;
    }

    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_SERVERLET, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
