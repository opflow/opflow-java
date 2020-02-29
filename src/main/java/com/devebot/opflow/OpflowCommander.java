package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.annotation.OpflowSourceRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import com.devebot.opflow.exception.OpflowRpcRegistrationException;
import com.devebot.opflow.exception.OpflowWorkerNotFoundException;
import com.devebot.opflow.supports.OpflowConcurrentMap;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowSysInfo;
import io.undertow.server.RoutingHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowCommander implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();

    public final static List<String> SERVICE_BEAN_NAMES = Arrays.asList(new String[] {
        CONST.COMPNAME_CONFIGURER, CONST.COMPNAME_PUBLISHER, CONST.COMPNAME_RPC_MASTER
    });

    public final static List<String> SUPPORT_BEAN_NAMES = Arrays.asList(new String[] {
        CONST.COMPNAME_REQ_EXTRACTOR, CONST.COMPNAME_RESTRICTOR, CONST.COMPNAME_RPC_WATCHER,
        CONST.COMPNAME_SPEED_METER, CONST.COMPNAME_PROM_EXPORTER, CONST.COMPNAME_REST_SERVER
    });

    public final static List<String> ALL_BEAN_NAMES = OpflowUtil.mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);

    public final static String PARAM_RESERVED_WORKER_ENABLED = "reservedWorkerEnabled";

    public final static boolean KEEP_LOGIC_CLEARLY = false;
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommander.class);
    
    private final boolean strictMode;
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowThroughput.Meter speedMeter;
    private final OpflowConfig.Loader configLoader;

    private OpflowRestrictorMaster restrictor;
    
    private boolean reservedWorkerEnabled;
    private OpflowPubsubHandler configurer;
    private OpflowRpcMaster rpcMaster;
    private OpflowPubsubHandler publisher;
    private OpflowRpcChecker rpcChecker;
    private OpflowRpcWatcher rpcWatcher;
    private OpflowRpcObserverListener rpcObserver;
    
    private OpflowRestServer restServer;
    private OpflowReqExtractor reqExtractor;

    public OpflowCommander() throws OpflowBootstrapException {
        this(null, null);
    }
    
    public OpflowCommander(OpflowConfig.Loader loader) throws OpflowBootstrapException {
        this(loader, null);
    }

    public OpflowCommander(Map<String, Object> kwargs) throws OpflowBootstrapException {
        this(null, kwargs);
    }

    private OpflowCommander(OpflowConfig.Loader loader, Map<String, Object> kwargs) throws OpflowBootstrapException {
        if (loader != null) {
            configLoader = loader;
        } else {
            configLoader = null;
        }
        if (configLoader != null) {
            kwargs = configLoader.loadConfiguration();
        }
        kwargs = OpflowUtil.ensureNotNull(kwargs);
        strictMode = OpflowObjectTree.getOptionValue(kwargs, "strictMode", Boolean.class, Boolean.FALSE);
        componentId = OpflowUtil.getOptionField(kwargs, CONST.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].new()")
                .stringify());
        
        measurer = OpflowPromMeasurer.getInstance((Map<String, Object>) kwargs.get(CONST.COMPNAME_PROM_EXPORTER));
        OpflowPromMeasurer.RpcInvocationCounter counter = measurer.getRpcInvocationCounter(CONST.COMPNAME_COMMANDER);
        
        Map<String, Object> speedMeterCfg = (Map<String, Object>) kwargs.get(CONST.COMPNAME_SPEED_METER);
        
        if (speedMeterCfg == null || OpflowUtil.isComponentEnabled(speedMeterCfg)) {
            speedMeter = (new OpflowThroughput.Meter(speedMeterCfg))
                    .register(OpflowPromMeasurer.LABEL_RPC_DIRECT_WORKER, counter.getDirectWorkerInfoSource())
                    .register(OpflowPromMeasurer.LABEL_RPC_REMOTE_WORKER, counter.getRemoteWorkerInfoSource());
        } else {
            speedMeter = null;
        }
        
        Map<String, Object> restrictorCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_RESTRICTOR);
        
        if (restrictorCfg == null || OpflowUtil.isComponentEnabled(restrictorCfg)) {
            restrictor = new OpflowRestrictorMaster(OpflowObjectTree.buildMap(restrictorCfg)
                    .put(CONST.COMPONENT_ID, componentId)
                    .toMap());
        }
        
        if (restrictor != null) {
            restrictor.block();
        }
        
        this.init(kwargs);
        
        measurer.updateComponentInstance(CONST.COMPNAME_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.INC);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].new() end!")
                .stringify());
    }
    
    private void init(Map<String, Object> kwargs) throws OpflowBootstrapException {
        if (kwargs.get(PARAM_RESERVED_WORKER_ENABLED) != null && kwargs.get(PARAM_RESERVED_WORKER_ENABLED) instanceof Boolean) {
            reservedWorkerEnabled = (Boolean) kwargs.get(PARAM_RESERVED_WORKER_ENABLED);
        } else {
            reservedWorkerEnabled = true;
        }

        Map<String, Object> reqExtractorCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_REQ_EXTRACTOR);
        Map<String, Object> configurerCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_CONFIGURER);
        Map<String, Object> rpcMasterCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_RPC_MASTER);
        Map<String, Object> publisherCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_PUBLISHER);
        Map<String, Object> rpcObserverCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_RPC_OBSERVER);
        Map<String, Object> rpcWatcherCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_RPC_WATCHER);
        Map<String, Object> restServerCfg = (Map<String, Object>)kwargs.get(CONST.COMPNAME_REST_SERVER);

        HashSet<String> checkExchange = new HashSet<>();

        if (OpflowUtil.isComponentEnabled(configurerCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(configurerCfg)) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(configurerCfg))) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters");
            }
        }

        if (OpflowUtil.isComponentEnabled(rpcMasterCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(rpcMasterCfg)) {
                throw new OpflowBootstrapException("Invalid RpcMaster connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(rpcMasterCfg))) {
                throw new OpflowBootstrapException("Duplicated RpcMaster connection parameters");
            }
        }

        if (OpflowUtil.isComponentEnabled(publisherCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(publisherCfg)) {
                throw new OpflowBootstrapException("Invalid Publisher connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(publisherCfg))) {
                throw new OpflowBootstrapException("Duplicated Publisher connection parameters");
            }
        }

        try {
            if (reqExtractorCfg == null || OpflowUtil.isComponentEnabled(reqExtractorCfg)) {
                reqExtractor = new OpflowReqExtractor(reqExtractorCfg);
            }

            if (rpcObserverCfg == null || OpflowUtil.isComponentEnabled(rpcObserverCfg)) {
                rpcObserver = new OpflowRpcObserverListener();
            }

            if (OpflowUtil.isComponentEnabled(configurerCfg)) {
                configurer = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(CONST.COMPNAME_MEASURER, measurer);
                    }
                }, configurerCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(rpcMasterCfg)) {
                rpcMaster = new OpflowRpcMaster(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(CONST.COMPNAME_MEASURER, measurer);
                        opts.put(CONST.COMPNAME_RPC_OBSERVER, rpcObserver);
                    }
                }, rpcMasterCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(publisherCfg)) {
                publisher = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(CONST.COMPNAME_MEASURER, measurer);
                    }
                }, publisherCfg).toMap());
            }

            rpcChecker = new OpflowRpcCheckerMaster(restrictor.getValveRestrictor(), rpcMaster);

            rpcWatcher = new OpflowRpcWatcher(rpcChecker, OpflowObjectTree.buildMap(rpcWatcherCfg)
                    .put(CONST.COMPONENT_ID, componentId)
                    .toMap());

            OpflowInfoCollector infoCollector = new OpflowInfoCollectorMaster(componentId, measurer, restrictor, rpcMaster, handlers, rpcObserver, rpcWatcher, speedMeter);

            OpflowTaskSubmitter taskSubmitter = new OpflowTaskSubmitterMaster(componentId, measurer, restrictor, rpcMaster, handlers, speedMeter);
            
            restServer = new OpflowRestServer(infoCollector, taskSubmitter, rpcChecker, OpflowObjectTree.buildMap(restServerCfg)
                    .put(CONST.COMPONENT_ID, componentId)
                    .toMap());
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
    }
    
    public boolean isReservedWorkerEnabled() {
        return this.reservedWorkerEnabled;
    }
    
    public void setReservedWorkerEnabled(boolean enabled) {
        this.reservedWorkerEnabled = enabled;
    }

    public RoutingHandler getDefaultHandlers() {
        if (restServer != null) {
            return restServer.getDefaultHandlers();
        }
        return null;
    }
    
    public void ping(String query) throws Throwable {
        rpcChecker.send(new OpflowRpcChecker.Ping(query));
    }
    
    public final void serve() {
        serve(null);
    }

    public final void serve(RoutingHandler httpHandlers) {
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}].serve() begin")
                .stringify());
        
        OpflowUUID.start();
        
        if (rpcWatcher != null) {
            rpcWatcher.start();
        }
        if (restServer != null) {
            if (httpHandlers == null) {
                restServer.serve();
            } else {
                restServer.serve(httpHandlers);
            }
        }
        if (restrictor != null) {
            restrictor.unblock();
        }
        if (speedMeter != null) {
            speedMeter.start();
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}].serve() end")
                .stringify());
    }

    @Override
    public final void close() {
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].close()")
                .stringify());

        if (restrictor != null) {
            restrictor.block();
        }

        if (speedMeter != null) {
            speedMeter.close();
        }

        if (restServer != null) restServer.close();
        if (rpcWatcher != null) rpcWatcher.close();

        if (publisher != null) publisher.close();
        if (rpcMaster != null) rpcMaster.close();
        if (configurer != null) configurer.close();

        if (restrictor != null) {
            restrictor.close();
        }

        OpflowUUID.release();

        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].close() end!")
                .stringify());
    }
    
    private static class OpflowRestrictorMaster extends OpflowRestrictable.Runner implements AutoCloseable {
        private final static Logger LOG = LoggerFactory.getLogger(OpflowRestrictorMaster.class);

        protected final String componentId;
        protected final OpflowLogTracer logTracer;

        private final OpflowRestrictor.OnOff onoffRestrictor;
        private final OpflowRestrictor.Valve valveRestrictor;
        private final OpflowRestrictor.Pause pauseRestrictor;
        private final OpflowRestrictor.Limit limitRestrictor;

        public OpflowRestrictorMaster(Map<String, Object> options) {
            options = OpflowUtil.ensureNotNull(options);

            componentId = OpflowUtil.getOptionField(options, CONST.COMPONENT_ID, true);
            logTracer = OpflowLogTracer.ROOT.branch("restrictorId", componentId);

            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("Restrictor[${restrictorId}].new()")
                    .stringify());

            onoffRestrictor = new OpflowRestrictor.OnOff(options);
            valveRestrictor = new OpflowRestrictor.Valve(options);
            pauseRestrictor = new OpflowRestrictor.Pause(options);
            limitRestrictor = new OpflowRestrictor.Limit(options);

            super.append(onoffRestrictor.setLogTracer(logTracer));
            super.append(valveRestrictor.setLogTracer(logTracer));
            super.append(pauseRestrictor.setLogTracer(logTracer));
            super.append(limitRestrictor.setLogTracer(logTracer));

            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("Restrictor[${restrictorId}].new() end!")
                    .stringify());
        }

        public String getComponentId() {
            return componentId;
        }

        public OpflowRestrictor.Valve getValveRestrictor() {
            return valveRestrictor;
        }

        public boolean isActive() {
            return onoffRestrictor.isActive();
        }

        public void setActive(boolean enabled) {
            onoffRestrictor.setActive(enabled);
        }

        public boolean isBlocked() {
            return valveRestrictor.isBlocked();
        }

        public void block() {
            valveRestrictor.block();
        }

        public void unblock() {
            valveRestrictor.unblock();
        }

        public boolean isPauseEnabled() {
            return pauseRestrictor.isPauseEnabled();
        }

        public long getPauseTimeout() {
            return pauseRestrictor.getPauseTimeout();
        }

        public long getPauseDuration() {
            return pauseRestrictor.getPauseDuration();
        }

        public long getPauseElapsed() {
            return pauseRestrictor.getPauseElapsed();
        }

        public boolean isPaused() {
            return pauseRestrictor.isPaused();
        }

        public Map<String, Object> pause(long duration) {
            return pauseRestrictor.pause(duration);
        }

        public Map<String, Object> unpause() {
            return pauseRestrictor.unpause();
        }

        public int getSemaphoreLimit() {
            return limitRestrictor.getSemaphoreLimit();
        }

        public int getSemaphorePermits() {
            return limitRestrictor.getSemaphorePermits();
        }

        public boolean isSemaphoreEnabled() {
            return limitRestrictor.isSemaphoreEnabled();
        }

        public long getSemaphoreTimeout() {
            return limitRestrictor.getSemaphoreTimeout();
        }

        @Override
        public void close() {
            pauseRestrictor.close();
        }
    }

    private static class OpflowRpcCheckerMaster extends OpflowRpcChecker {

        private final static String DEFAULT_BALL_JSON = OpflowJsonTool.toString(new Object[] { new Ping() });

        private final OpflowRestrictor.Valve restrictor;
        private final OpflowRpcMaster rpcMaster;

        OpflowRpcCheckerMaster(OpflowRestrictor.Valve restrictor, OpflowRpcMaster rpcMaster) throws OpflowBootstrapException {
            this.restrictor = restrictor;
            this.rpcMaster = rpcMaster;
        }

        @Override
        public Pong send(final Ping ping) throws Throwable {
            if (this.restrictor == null) {
                return _send_safe(ping);
            }
            return this.restrictor.filter(new OpflowRestrictor.Action<Pong>() {
                @Override
                public Pong process() throws Throwable {
                    return _send_safe(ping);
                }
            });
        }
        private Pong _send_safe(final Ping ping) throws Throwable {
            Date startTime = new Date();
            String body = (ping == null) ? DEFAULT_BALL_JSON : OpflowJsonTool.toString(new Object[] { ping });
            String routineId = OpflowUUID.getBase64ID();
            String routineTimestamp = OpflowDateTime.toISO8601UTC(startTime);
            OpflowRpcRequest rpcRequest = rpcMaster.request(getSendMethodName(), body, (new OpflowRpcParameter(routineId, routineTimestamp))
                    .setProgressEnabled(false)
                    .setRoutineScope("internal"));
            OpflowRpcResult rpcResult = rpcRequest.extractResult(false);
            Date endTime = new Date();

            if (rpcResult.isTimeout()) {
                throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
            }

            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            Pong pong = OpflowJsonTool.toObject(rpcResult.getValueAsString(), Pong.class);
            pong.getParameters().put("routineId", routineId);
            pong.getParameters().put("startTime", startTime);
            pong.getParameters().put("endTime", endTime);
            pong.getParameters().put("elapsedTime", endTime.getTime() - startTime.getTime());
            return pong;
        }
    }

    private static class OpflowRpcObserverListener implements OpflowRpcObserver.Listener {

        private final long keepAliveTimeout = 30000;
        private final OpflowConcurrentMap<String, OpflowRpcObserver.Manifest> manifests = new OpflowConcurrentMap<>();

        public OpflowRpcObserverListener() {
            
        }

        @Override
        public void check(String componentId, String version, String payload) {
            OpflowRpcObserver.Manifest manifest = null;
            if (componentId != null) {
                // assure the manifest object
                if (manifests.containsKey(componentId)) {
                    manifest = manifests.get(componentId);
                } else {
                    manifest = new OpflowRpcObserver.Manifest(componentId);
                    manifests.put(componentId, manifest);
                }
                manifest.touch();
                // update the compatible status
                if (version == null) {
                    manifest.setCompatible(CONST.LEGACY_SUPPORT_ENABLED);
                } else {
                    if (version.equals(CONST.AMQP_PROTOCOL_VERSION)) {
                        manifest.setCompatible(true);
                    } else {
                        manifest.setCompatible((version.equals("0") && CONST.LEGACY_SUPPORT_ENABLED));
                    }
                }
            }
        }

        public Collection<OpflowRpcObserver.Manifest> rollup() {
            Set<String> keys = manifests.keySet();
            for (String key: keys) {
                // refresh the state of the manifest
                OpflowRpcObserver.Manifest manifest = manifests.get(key);
                manifest.refresh();
                // validate the state of the manifest
                if (manifest.getLosingTouchDuration() > keepAliveTimeout) {
                    manifests.remove(key);
                }
            }
            return manifests.values();
        }

        public Object getInformation() {
            return this.rollup();
        }
    }

    private static class OpflowTaskSubmitterMaster implements OpflowTaskSubmitter {

        private final String componentId;
        private final OpflowPromMeasurer measurer;
        private final OpflowLogTracer logTracer;
        private final OpflowRestrictorMaster restrictor;
        private final OpflowRpcMaster rpcMaster;
        private final Map<String, RpcInvocationHandler> handlers;
        private final OpflowThroughput.Meter speedMeter;
        
        public OpflowTaskSubmitterMaster(String componentId,
                OpflowPromMeasurer measurer,
                OpflowRestrictorMaster restrictor,
                OpflowRpcMaster rpcMaster,
                Map<String, RpcInvocationHandler> mappings,
                OpflowThroughput.Meter speedMeter
        ) {
            this.componentId = componentId;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.rpcMaster = rpcMaster;
            this.handlers = mappings;
            this.speedMeter = speedMeter;
            this.logTracer = OpflowLogTracer.ROOT.branch("taskSubmitterId", componentId);
        }
        
        @Override
        public Map<String, Object> pause(long duration) {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].pause(true) is invoked")
                    .stringify());
            if (restrictor == null) {
                return OpflowObjectTree.buildMap()
                        .toMap();
            }
            return restrictor.pause(duration);
        }
        
        @Override
        public Map<String, Object> unpause() {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].unpause() is invoked")
                    .stringify());
            if (restrictor == null) {
                return OpflowObjectTree.buildMap()
                        .toMap();
            }
            return restrictor.unpause();
        }
        
        @Override
        public Map<String, Object> reset() {
            if (rpcMaster == null) {
                return OpflowObjectTree.buildMap()
                        .toMap();
            }
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].reset() is invoked")
                    .stringify());
            rpcMaster.reset();
            return OpflowObjectTree.buildMap()
                    .toMap();
        }
        
        @Override
        public Map<String, Object> resetRpcInvocationCounter() {
            if (measurer != null) {
                measurer.resetRpcInvocationCounter();
            }
            if (speedMeter != null) {
                speedMeter.reset();
            }
            if (rpcMaster != null) {
                rpcMaster.resetCallbackQueueCounter();
            }
            return OpflowObjectTree.buildMap().put("acknowledged", true).toMap();
        }
        
        @Override
        public Map<String, Object> activateDetachedWorker(boolean state, Map<String, Object> opts) {
            return activateWorker(CONST.COMPNAME_REMOTE_WORKER, state, opts);
        }
        
        @Override
        public Map<String, Object> activateReservedWorker(boolean state, Map<String, Object> opts) {
            return activateWorker(CONST.COMPNAME_NATIVE_WORKER, state, opts);
        }
        
        private Map<String, Object> activateWorker(String type, boolean state, Map<String, Object> opts) {
            String clazz = (String) OpflowUtil.getOptionField(opts, "class", null);
            for(final Map.Entry<String, RpcInvocationHandler> entry : handlers.entrySet()) {
                final String key = entry.getKey();
                final RpcInvocationHandler handler = entry.getValue();
                if (clazz != null) {
                    if (clazz.equals(key)) {
                        activateWorkerForRpcInvocation(handler, type, state);
                    }
                } else {
                    activateWorkerForRpcInvocation(handler, type, state);
                }
            }
            return OpflowObjectTree.buildMap()
                    .put("mappings", OpflowInfoCollectorMaster.renderRpcInvocationHandlers(handlers))
                    .toMap();
        }
        
        private void activateWorkerForRpcInvocation(RpcInvocationHandler handler, String type, boolean state) {
            if (CONST.COMPNAME_REMOTE_WORKER.equals(type)) {
                handler.setDetachedWorkerActive(state);
                return;
            }
            if (CONST.COMPNAME_NATIVE_WORKER.equals(type)) {
                handler.setReservedWorkerActive(state);
                return;
            }
        }
    }

    private static class OpflowInfoCollectorMaster implements OpflowInfoCollector {
        private final String componentId;
        private final OpflowPromMeasurer measurer;
        private final OpflowRestrictorMaster restrictor;
        private final OpflowRpcWatcher rpcWatcher;
        private final OpflowRpcMaster rpcMaster;
        private final Map<String, RpcInvocationHandler> handlers;
        private final OpflowRpcObserverListener rpcObserver;
        private final OpflowThroughput.Meter speedMeter;
        private final Date startTime;

        public OpflowInfoCollectorMaster(String componentId,
                OpflowPromMeasurer measurer,
                OpflowRestrictorMaster restrictor,
                OpflowRpcMaster rpcMaster,
                Map<String, RpcInvocationHandler> mappings,
                OpflowRpcObserverListener rpcObserver,
                OpflowRpcWatcher rpcWatcher,
                OpflowThroughput.Meter speedMeter
        ) {
            this.componentId = componentId;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.rpcWatcher = rpcWatcher;
            this.rpcMaster = rpcMaster;
            this.handlers = mappings;
            this.rpcObserver = rpcObserver;
            this.speedMeter = speedMeter;
            this.startTime = new Date();
        }

        @Override
        public Map<String, Object> collect() {
            return collect(new HashMap<>());
        }

        @Override
        public Map<String, Object> collect(String scope) {
            return collect(OpflowObjectTree.<Boolean>buildMap()
                    .put((scope == null) ? SCOPE_PING : scope, true)
                    .toMap());
        }

        private boolean checkOption(Map<String, Boolean> options, String optionName) {
            Boolean opt = options.get(optionName);
            return opt != null && opt;
        }

        @Override
        public Map<String, Object> collect(Map<String, Boolean> options) {
            final Map<String, Boolean> flag = (options != null) ? options : new HashMap<String, Boolean>();
            
            OpflowObjectTree.Builder root = OpflowObjectTree.buildMap();
            
            root.put(CONST.COMPNAME_COMMANDER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put(CONST.COMPONENT_ID, componentId);
                    
                    // rpcMaster information
                    if (rpcMaster != null) {
                        opts.put(CONST.COMPNAME_RPC_MASTER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                OpflowEngine engine = rpcMaster.getEngine();
                                
                                opt2.put(CONST.COMPONENT_ID, rpcMaster.getComponentId());
                                opt2.put("applicationId", engine.getApplicationId());
                                opt2.put("exchangeName", engine.getExchangeName());

                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put("exchangeDurable", engine.getExchangeDurable());
                                }

                                opt2.put("routingKey", engine.getRoutingKey());

                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put("otherKeys", engine.getOtherKeys());
                                }

                                opt2.put("callbackQueue", rpcMaster.getCallbackName());

                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put("callbackDurable", rpcMaster.getCallbackDurable());
                                    opt2.put("callbackExclusive", rpcMaster.getCallbackExclusive());
                                    opt2.put("callbackAutoDelete", rpcMaster.getCallbackAutoDelete());
                                }

                                opt2.put("request", OpflowObjectTree.buildMap()
                                        .put("expiration", rpcMaster.getExpiration())
                                        .toMap());

                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put("transport", CONST.getProtocolInfo());
                                }
                            }
                        }).toMap());
                    }
                    
                    // RPC mappings
                    if (checkOption(flag, SCOPE_INFO)) {
                        opts.put("mappings", renderRpcInvocationHandlers(handlers));
                    }
                    
                    // RPC current workers
                    if (checkOption(flag, SCOPE_INFO)) {
                        if (rpcObserver != null) {
                            opts.put(CONST.COMPNAME_RPC_WORKER, rpcObserver.getInformation());
                        }
                    }
                    
                    // RpcWatcher information
                    if (checkOption(flag, SCOPE_INFO)) {
                        opts.put(CONST.COMPNAME_RPC_WATCHER, OpflowObjectTree.buildMap()
                                .put("enabled", rpcWatcher.isEnabled())
                                .put("interval", rpcWatcher.getInterval())
                                .put("count", rpcWatcher.getCount())
                                .put("congestive", rpcWatcher.isCongested())
                                .toMap());
                    }
                    
                    // restrictor information
                    if (checkOption(flag, SCOPE_INFO)) {
                        if (restrictor != null) {
                            opts.put(CONST.COMPNAME_RESTRICTOR, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                                @Override
                                public void transform(Map<String, Object> opt2) {
                                    int availablePermits = restrictor.getSemaphorePermits();
                                    opt2.put("pauseEnabled", restrictor.isPauseEnabled());
                                    opt2.put("pauseTimeout", restrictor.getPauseTimeout());
                                    boolean isPaused = restrictor.isPaused();
                                    opt2.put("pauseStatus", isPaused ? "on" : "off");
                                    if (isPaused) {
                                        opt2.put("pauseElapsed", restrictor.getPauseElapsed());
                                        opt2.put("pauseDuration", restrictor.getPauseDuration());
                                    }
                                    opt2.put("semaphoreLimit", restrictor.getSemaphoreLimit());
                                    opt2.put("semaphoreUsedPermits", restrictor.getSemaphoreLimit() - availablePermits);
                                    opt2.put("semaphoreFreePermits", availablePermits);
                                    opt2.put("semaphoreEnabled", restrictor.isSemaphoreEnabled());
                                    opt2.put("semaphoreTimeout", restrictor.getSemaphoreTimeout());
                                }
                            }).toMap());
                        } else {
                            opts.put(CONST.COMPNAME_RESTRICTOR, OpflowObjectTree.buildMap()
                                    .put("enabled", false)
                                    .toMap());
                        }
                    }
                }
            }).toMap());
            
            // start-time & uptime
            if (checkOption(flag, SCOPE_INFO)) {
                Date currentTime = new Date();
                root.put("miscellaneous", OpflowObjectTree.buildMap()
                        .put("threadCount", Thread.activeCount())
                        .put("startTime", startTime)
                        .put("currentTime", currentTime)
                        .put("uptime", OpflowDateTime.printElapsedTime(startTime, currentTime))
                        .toMap());
            }
            
            // git commit information
            if (checkOption(flag, SCOPE_INFO)) {
                root.put("source-code-info", OpflowObjectTree.buildMap()
                        .put("server", OpflowSysInfo.getGitInfo("META-INF/scm/service-master/git-info.json"))
                        .put(CONST.FRAMEWORK_ID, OpflowSysInfo.getGitInfo())
                        .toMap());
            }
            
            return root.toMap();
        }
        
        @Override
        public Map<String, Object> traffic(Map<String, Boolean> options) {
            final Map<String, Boolean> flag = (options != null) ? options : new HashMap<String, Boolean>();
            
            Map<String, Object> metrics = OpflowObjectTree.buildMap().toMap();
            
            // update the RPC invocation counters
            if (measurer != null) {
                OpflowPromMeasurer.RpcInvocationCounter counter = measurer.getRpcInvocationCounter(CONST.COMPNAME_COMMANDER);
                if (counter != null) {
                    OpflowObjectTree.merge(metrics, counter.toMap(true, checkOption(flag, SCOPE_MESSAGE_RATE)));
                }
            }
            
            // update the RPC invocation throughput
            if (speedMeter != null && checkOption(flag, SCOPE_THROUGHPUT)) {
                if (speedMeter.isActive()) {
                    if (checkOption(flag, SCOPE_LATEST_SPEED)) {
                        OpflowObjectTree.merge(metrics, speedMeter.export(1));
                    } else {
                        OpflowObjectTree.merge(metrics, speedMeter.export());
                    }
                }
            }
            
            // size of the callback queue
            if (KEEP_LOGIC_CLEARLY) {
                OpflowObjectTree.merge(metrics, OpflowObjectTree.buildMap()
                        .put(OpflowPromMeasurer.LABEL_RPC_REMOTE_WORKER, OpflowObjectTree.buildMap()
                                .put("waitingReqTotal", OpflowObjectTree.buildMap()
                                        .put("current", rpcMaster.getActiveRequestTotal())
                                        .put("top", rpcMaster.getMaxWaitingRequests())
                                        .toMap())
                                .toMap())
                        .toMap());
            } else {
                Map<String, Object> parentOfQueueInfo;
                if (metrics.containsKey(OpflowPromMeasurer.LABEL_RPC_REMOTE_WORKER)) {
                    parentOfQueueInfo = (Map<String, Object>) metrics.get(OpflowPromMeasurer.LABEL_RPC_REMOTE_WORKER);
                } else {
                    parentOfQueueInfo = OpflowObjectTree.buildMap().toMap();
                    metrics.put(OpflowPromMeasurer.LABEL_RPC_REMOTE_WORKER, parentOfQueueInfo);
                }
                Map<String, Object> rpcWaitingRequests = OpflowObjectTree.buildMap()
                        .put("current", rpcMaster.getActiveRequestTotal())
                        .put("top", rpcMaster.getMaxWaitingRequests())
                        .toMap();
                parentOfQueueInfo.put("waitingReqTotal", rpcWaitingRequests);
            }
            
            return OpflowObjectTree.buildMap()
                    .put("metadata", speedMeter.getMetadata())
                    .put("metrics", metrics)
                    .toMap();
        }
        
        protected static List<Map<String, Object>> renderRpcInvocationHandlers(Map<String, RpcInvocationHandler> handlers) {
            List<Map<String, Object>> mappingInfos = new ArrayList<>();
            for(final Map.Entry<String, RpcInvocationHandler> entry : handlers.entrySet()) {
                final RpcInvocationHandler val = entry.getValue();
                mappingInfos.add(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("class", entry.getKey());
                        opts.put("methods", val.getMethodNames());
                        opts.put("isReservedWorkerActive", val.isReservedWorkerActive());
                        opts.put("isReservedWorkerAvailable", val.isReservedWorkerAvailable());
                        opts.put("isDetachedWorkerActive", val.isDetachedWorkerActive());
                        if (val.getReservedWorkerClassName() != null) {
                            opts.put("reservedWorkerClassName", val.getReservedWorkerClassName());
                        }
                    }
                }).toMap());
            }
            return mappingInfos;
        }
    }

    private static class RpcInvocationHandler implements InvocationHandler {
        private final OpflowLogTracer logTracer;
        private final OpflowPromMeasurer measurer;
        private final OpflowRestrictorMaster restrictor;
        private final OpflowReqExtractor reqExtractor;
        private final OpflowRpcWatcher rpcWatcher;
        
        private final OpflowRpcMaster rpcMaster;
        private final OpflowPubsubHandler publisher;
        
        private final Class clazz;
        private final Object reservedWorker;
        private final boolean reservedWorkerEnabled;
        private final Map<String, String> aliasOfMethod = new HashMap<>();
        private final Map<String, Boolean> methodIsAsync = new HashMap<>();
        
        private boolean detachedWorkerActive = true;
        private boolean reservedWorkerActive = true;
        
        public RpcInvocationHandler(OpflowLogTracer logTracer, OpflowPromMeasurer measurer,
                OpflowRestrictorMaster restrictor, OpflowReqExtractor reqExtractor, OpflowRpcWatcher rpcWatcher,
                OpflowRpcMaster rpcMaster, OpflowPubsubHandler publisher,
                Class clazz, Object reservedWorker, boolean reservedWorkerEnabled
        ) {
            this.logTracer = logTracer;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.reqExtractor = reqExtractor;
            this.rpcWatcher = rpcWatcher;
            
            this.rpcMaster = rpcMaster;
            this.publisher = publisher;
            
            this.clazz = clazz;
            this.reservedWorker = reservedWorker;
            this.reservedWorkerEnabled = reservedWorkerEnabled;
            for (Method method : this.clazz.getDeclaredMethods()) {
                String methodSignature = OpflowUtil.getMethodSignature(method);
                OpflowSourceRoutine routine = OpflowUtil.extractMethodAnnotation(method, OpflowSourceRoutine.class);
                if (routine != null && routine.alias() != null && routine.alias().length() > 0) {
                    String alias = routine.alias();
                    if (aliasOfMethod.containsValue(alias)) {
                        throw new OpflowInterceptionException("Alias[" + alias + "]/methodSignature[" + methodSignature + "] is duplicated");
                    }
                    aliasOfMethod.put(methodSignature, alias);
                    if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                            .put("alias", alias)
                            .put("methodSignature", methodSignature)
                            .text("link alias to methodSignature")
                            .stringify());
                }
                methodIsAsync.put(methodSignature, (routine != null) && routine.isAsync());
            }
        }

        public Set<String> getMethodNames() {
            return methodIsAsync.keySet();
        }

        public boolean isDetachedWorkerActive() {
            return detachedWorkerActive;
        }

        public void setDetachedWorkerActive(boolean detachedWorkerActive) {
            this.detachedWorkerActive = detachedWorkerActive;
        }

        public boolean isReservedWorkerActive() {
            return reservedWorkerActive;
        }

        public void setReservedWorkerActive(boolean reservedWorkerActive) {
            this.reservedWorkerActive = reservedWorkerActive;
        }

        public boolean isReservedWorkerAvailable() {
            return this.reservedWorker != null && this.reservedWorkerEnabled && this.reservedWorkerActive;
        }

        public String getReservedWorkerClassName() {
            if (this.reservedWorker == null) return null;
            return this.reservedWorker.getClass().getName();
        }
        
        public Integer getReservedWorkerHashCode() {
            if (this.reservedWorker == null) return null;
            return this.reservedWorker.hashCode();
        }
        
        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            if (this.restrictor == null) {
                return _invoke(proxy, method, args);
            }
            return this.restrictor.filter(new OpflowRestrictor.Action<Object>() {
                @Override
                public Object process() throws Throwable {
                    return _invoke(proxy, method, args);
                }
            });
        }
        
        private Object _invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // generate the routineId
            final String routineId = OpflowUUID.getBase64ID();
            
            // generate the routineTimestamp
            final String routineTimestamp = OpflowDateTime.getCurrentTimeString();

            // create the logTracer
            final OpflowLogTracer reqTracer = logTracer.branch(CONST.REQUEST_TIME, routineTimestamp).branch(CONST.REQUEST_ID, routineId);

            // get the method signature
            String methodSignature = OpflowUtil.getMethodSignature(method);
            
            // convert the method signature to routineSignature
            String routineSignature = aliasOfMethod.getOrDefault(methodSignature, methodSignature);

            // determine the requestId
            final String requestId;
            if (reqExtractor != null) {
                String _requestId = reqExtractor.extractRequestId(args);
                requestId = (_requestId != null) ? _requestId : "REQ:" + routineId;
            } else {
                requestId = null;
            }

            Boolean isAsync = methodIsAsync.getOrDefault(methodSignature, false);
            if (reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                    .put("isAsync", isAsync)
                    .put("externalRequestId", requestId)
                    .put("methodSignature", methodSignature)
                    .put("routineSignature", routineSignature)
                    .text("Request[${requestId}][${requestTime}][x-commander-invocation-begin]" +
                            " - Commander[${commanderId}][${instanceId}]" +
                            " - method[${routineSignature}] is async [${isAsync}] with requestId[${externalRequestId}]")
                    .stringify());

            if (args == null) args = new Object[0];
            String body = OpflowJsonTool.toString(args);

            if (reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                    .put("args", args)
                    .put("body", body)
                    .text("Request[${requestId}][${requestTime}] - RpcInvocationHandler.invoke() details")
                    .stringify());

            if (this.publisher != null && isAsync && void.class.equals(method.getReturnType())) {
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-publish-method] - RpcInvocationHandler.invoke() dispatch the call to the publisher")
                        .stringify());
                measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, CONST.RPC_INVOCATION_FLOW_PUBLISHER, routineSignature, "begin");
                this.publisher.publish(body, OpflowObjectTree.buildMap(false)
                        .put(CONST.AMQP_HEADER_ROUTINE_ID, routineId)
                        .put(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, routineTimestamp)
                        .put(CONST.AMQP_HEADER_ROUTINE_SIGNATURE, routineSignature)
                        .toMap());
                return null;
            } else {
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-dispatch-method] - RpcInvocationHandler.invoke() dispatch the call to the rpcMaster")
                        .stringify());
                measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, CONST.RPC_INVOCATION_FLOW_RPC_MASTER, routineSignature, "begin");
            }
            
            // rpc switching
            if (rpcWatcher.isCongested() || !detachedWorkerActive) {
                if (this.isReservedWorkerAvailable()) {
                    if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                            .text("Request[${requestId}][${requestTime}][x-commander-reserved-worker-retain] - RpcInvocationHandler.invoke() retains the reservedWorker")
                            .stringify());
                    measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, "reserved_worker", routineSignature, "retain");
                    return method.invoke(this.reservedWorker, args);
                }
            }

            if (!detachedWorkerActive) {
                throw new OpflowWorkerNotFoundException("both reserved worker and detached worker are deactivated");
            }

            OpflowRpcRequest rpcSession = rpcMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                    .setProgressEnabled(false));
            OpflowRpcResult rpcResult = rpcSession.extractResult(false);

            if (rpcResult.isTimeout()) {
                rpcWatcher.setCongested(true);
                if (this.isReservedWorkerAvailable()) {
                    if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                            .text("Request[${requestId}][${requestTime}][x-commander-reserved-worker-rescue] - RpcInvocationHandler.invoke() rescues by the reservedWorker")
                            .stringify());
                    measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, "reserved_worker", routineSignature, "rescue");
                    return method.invoke(this.reservedWorker, args);
                }
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-detached-worker-timeout] - RpcInvocationHandler.invoke() is timeout")
                        .stringify());
                measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, "detached_worker", routineSignature, "timeout");
                throw new OpflowRequestTimeoutException();
            }

            if (rpcResult.isFailed()) {
                measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, "detached_worker", routineSignature, "failed");
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-detached-worker-failed] - RpcInvocationHandler.invoke() has failed")
                        .stringify());
                Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                    .put("returnType", method.getReturnType().getName())
                    .put("returnValue", rpcResult.getValueAsString())
                    .text("Request[${requestId}][${requestTime}][x-commander-detached-worker-ok] - RpcInvocationHandler.invoke() return the output")
                    .stringify());

            measurer.countRpcInvocation(CONST.COMPNAME_COMMANDER, "detached_worker", routineSignature, "ok");

            if (method.getReturnType() == void.class) return null;

            return OpflowJsonTool.toObject(rpcResult.getValueAsString(), method.getReturnType());
        }
    }

    private static Throwable rebuildInvokerException(Map<String, Object> errorMap) {
        Object exceptionName = errorMap.get("exceptionClass");
        Object exceptionPayload = errorMap.get("exceptionPayload");
        if (exceptionName != null && exceptionPayload != null) {
            try {
                Class exceptionClass = Class.forName(exceptionName.toString());
                return (Throwable) OpflowJsonTool.toObject(exceptionPayload.toString(), exceptionClass);
            } catch (ClassNotFoundException ex) {
                return rebuildFailureException(errorMap);
            }
        }
        return rebuildFailureException(errorMap);
    }

    private static Throwable rebuildFailureException(Map<String, Object> errorMap) {
        if (errorMap.get("message") != null) {
            return new OpflowRequestFailureException(errorMap.get("message").toString());
        }
        return new OpflowRequestFailureException();
    }

    private final Map<String, RpcInvocationHandler> handlers = new LinkedHashMap<>();

    private <T> RpcInvocationHandler getInvocationHandler(Class<T> clazz, T bean) {
        validateType(clazz);
        String clazzName = clazz.getName();
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                .put("className", clazzName)
                .text("getInvocationHandler() get InvocationHandler by type")
                .stringify());
        if (!handlers.containsKey(clazzName)) {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("className", clazzName)
                    .text("getInvocationHandler() InvocationHandler not found, create new one")
                    .stringify());
            handlers.put(clazzName, new RpcInvocationHandler(logTracer, measurer, restrictor, reqExtractor, rpcWatcher, 
                    rpcMaster, publisher, clazz, bean, reservedWorkerEnabled));
        } else {
            if (strictMode) {
                throw new OpflowRpcRegistrationException("Class [" + clazzName + "] has already registered");
            }
        }
        return handlers.get(clazzName);
    }

    private void removeInvocationHandler(Class clazz) {
        if (clazz == null) return;
        String clazzName = clazz.getName();
        handlers.remove(clazzName);
    }

    private boolean validateType(Class type) {
        boolean ok = true;
        if (OpflowUtil.isGenericDeclaration(type.toGenericString())) {
            ok = false;
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("typeString", type.toGenericString())
                    .text("generic types are unsupported")
                    .stringify());
        }
        Method[] methods = type.getDeclaredMethods();
        for(Method method:methods) {
            if (OpflowUtil.isGenericDeclaration(method.toGenericString())) {
                ok = false;
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .put("methodString", method.toGenericString())
                        .text("generic methods are unsupported")
                        .stringify());
            }
        }
        if (!ok) {
            throw new OpflowInterceptionException("Generic type/method is unsupported");
        }
        return ok;
    }

    public <T> T registerType(Class<T> type) {
        return registerType(type, null);
    }

    public <T> T registerType(Class<T> type, T bean) {
        if (type == null) {
            throw new OpflowInterceptionException("The [type] parameter must not be null");
        }
        if (OpflowRpcChecker.class.equals(type)) {
            throw new OpflowInterceptionException("Can not register the OpflowRpcChecker type");
        }
        try {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("className", type.getName())
                    .put("classLoaderName", type.getClassLoader().getClass().getName())
                    .text("registerType() calls newProxyInstance()")
                    .stringify());
            T t = (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type, bean));
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("className", type.getName())
                    .text("newProxyInstance() has completed")
                    .stringify());
            return t;
        } catch (IllegalArgumentException exception) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("newProxyInstance() has failed")
                    .stringify());
            throw new OpflowInterceptionException(exception);
        }
    }

    public <T> void unregisterType(Class<T> type) {
        removeInvocationHandler(type);
    }

    public Map<String, Object> getRpcInvocationCounter() {
        return measurer.getRpcInvocationCounter(CONST.COMPNAME_COMMANDER).toMap();
    }

    public void resetRpcInvocationCounter() {
        measurer.resetRpcInvocationCounter();
    }

    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(CONST.COMPNAME_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
