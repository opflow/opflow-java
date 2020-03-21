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
import com.devebot.opflow.supports.OpflowCollectionUtil;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowSysInfo;
import io.undertow.server.RoutingHandler;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
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

    private final static int FLAG_AMQP = 1;
    private final static int FLAG_HTTP = 2;
    
    public final static List<String> SERVICE_BEAN_NAMES = Arrays.asList(new String[] {
        OpflowConstant.COMP_CONFIGURER,
        OpflowConstant.COMP_PUBLISHER,
        OpflowConstant.COMP_RPC_AMQP_MASTER,
    });

    public final static List<String> SUPPORT_BEAN_NAMES = Arrays.asList(new String[] {
        OpflowConstant.COMP_REQ_EXTRACTOR,
        OpflowConstant.COMP_RESTRICTOR,
        OpflowConstant.COMP_RPC_HTTP_MASTER,
        OpflowConstant.COMP_RPC_WATCHER,
        OpflowConstant.COMP_SPEED_METER,
        OpflowConstant.COMP_PROM_EXPORTER,
        OpflowConstant.COMP_REST_SERVER,
    });

    public final static List<String> ALL_BEAN_NAMES = OpflowCollectionUtil.mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);

    public final static boolean KEEP_LOGIC_CLEARLY = false;
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommander.class);
    
    private final boolean strictMode;
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowThroughput.Meter speedMeter;
    private final OpflowConfig.Loader configLoader;

    private OpflowRestrictorMaster restrictor;
    
    private boolean nativeWorkerEnabled;
    private OpflowPubsubHandler configurer;
    private OpflowRpcAmqpMaster amqpMaster;
    private OpflowRpcHttpMaster httpMaster;
    private OpflowPubsubHandler publisher;
    private OpflowRpcChecker rpcChecker;
    private OpflowRpcWatcher rpcWatcher;
    private OpflowRpcObserver rpcObserver;
    
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
        
        kwargs = OpflowObjectTree.ensureNonNull(kwargs);
        
        strictMode = OpflowObjectTree.getOptionValue(kwargs, OpflowConstant.OPFLOW_COMMON_STRICT, Boolean.class, Boolean.FALSE);
        
        componentId = OpflowUtil.getOptionField(kwargs, CONST.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].new()")
                .stringify());
        
        measurer = OpflowPromMeasurer.getInstance(OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_PROM_EXPORTER));
        OpflowPromMeasurer.RpcInvocationCounter counter = measurer.getRpcInvocationCounter(OpflowConstant.COMP_COMMANDER);
        
        Map<String, Object> speedMeterCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_SPEED_METER);
        
        if (speedMeterCfg == null || OpflowUtil.isComponentEnabled(speedMeterCfg)) {
            speedMeter = (new OpflowThroughput.Meter(speedMeterCfg))
                    .register(OpflowPromMeasurer.LABEL_RPC_DIRECT_WORKER, counter.getNativeWorkerInfoSource())
                    .register(OpflowPromMeasurer.LABEL_RPC_REMOTE_AMQP_WORKER, counter.getRemoteAMQPWorkerInfoSource())
                    .register(OpflowPromMeasurer.LABEL_RPC_REMOTE_HTTP_WORKER, counter.getRemoteHTTPWorkerInfoSource());
        } else {
            speedMeter = null;
        }
        
        Map<String, Object> restrictorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RESTRICTOR);
        
        if (restrictorCfg == null || OpflowUtil.isComponentEnabled(restrictorCfg)) {
            restrictor = new OpflowRestrictorMaster(OpflowObjectTree.buildMap(restrictorCfg)
                    .put(CONST.COMPONENT_ID, componentId)
                    .toMap());
        }
        
        if (restrictor != null) {
            restrictor.block();
        }
        
        this.init(kwargs);
        
        measurer.updateComponentInstance(OpflowConstant.COMP_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.INC);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].new() end!")
                .stringify());
    }
    
    private void init(Map<String, Object> kwargs) throws OpflowBootstrapException {
        if (kwargs.get(OpflowConstant.PARAM_NATIVE_WORKER_ENABLED) instanceof Boolean) {
            nativeWorkerEnabled = (Boolean) kwargs.get(OpflowConstant.PARAM_NATIVE_WORKER_ENABLED);
        } else {
            nativeWorkerEnabled = true;
        }

        Map<String, Object> reqExtractorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_REQ_EXTRACTOR);
        Map<String, Object> configurerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_CONFIGURER);
        Map<String, Object> amqpMasterCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_AMQP_MASTER, OpflowConstant.COMP_CFG_AMQP_MASTER);
        Map<String, Object> httpMasterCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_HTTP_MASTER);
        Map<String, Object> publisherCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_PUBLISHER);
        Map<String, Object> rpcObserverCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_OBSERVER);
        Map<String, Object> rpcWatcherCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_WATCHER);
        Map<String, Object> restServerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_REST_SERVER);

        HashSet<String> checkExchange = new HashSet<>();

        if (OpflowUtil.isComponentEnabled(configurerCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(configurerCfg)) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(configurerCfg))) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters");
            }
        }

        if (OpflowUtil.isComponentEnabled(amqpMasterCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(amqpMasterCfg, OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY)) {
                throw new OpflowBootstrapException("Invalid RpcMaster connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(amqpMasterCfg, OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY))) {
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

            rpcObserver = new OpflowRpcObserver();

            if (OpflowUtil.isComponentEnabled(configurerCfg)) {
                configurer = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    }
                }, configurerCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(amqpMasterCfg)) {
                amqpMaster = new OpflowRpcAmqpMaster(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                        opts.put(OpflowConstant.COMP_RPC_OBSERVER, rpcObserver);
                    }
                }, amqpMasterCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(httpMasterCfg)) {
                httpMaster = new OpflowRpcHttpMaster(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                        opts.put(OpflowConstant.COMP_RPC_OBSERVER, rpcObserver);
                    }
                }, httpMasterCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(publisherCfg)) {
                publisher = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put(CONST.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    }
                }, publisherCfg).toMap());
            }

            rpcChecker = new OpflowRpcCheckerMaster(restrictor.getValveRestrictor(), rpcObserver, amqpMaster, httpMaster);

            rpcWatcher = new OpflowRpcWatcher(rpcChecker, OpflowObjectTree.buildMap(rpcWatcherCfg)
                    .put(CONST.COMPONENT_ID, componentId)
                    .toMap());

            rpcObserver.setKeepAliveTimeout(rpcWatcher.getInterval());

            OpflowInfoCollector infoCollector = new OpflowInfoCollectorMaster(componentId, measurer, restrictor, amqpMaster, httpMaster, handlers, speedMeter, rpcObserver, rpcWatcher);

            OpflowTaskSubmitter taskSubmitter = new OpflowTaskSubmitterMaster(componentId, measurer, restrictor, amqpMaster, httpMaster, handlers, speedMeter);

            restServer = new OpflowRestServer(infoCollector, taskSubmitter, rpcChecker, OpflowObjectTree.buildMap(restServerCfg)
                    .put(CONST.COMPONENT_ID, componentId)
                    .toMap());
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
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
        if (amqpMaster != null) amqpMaster.close();
        if (httpMaster != null) httpMaster.close();
        if (configurer != null) configurer.close();

        if (rpcObserver != null) rpcObserver.close();

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
            options = OpflowObjectTree.ensureNonNull(options);

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
        private final OpflowRpcAmqpMaster amqpMaster;
        private final OpflowRpcHttpMaster httpMaster;
        private final OpflowRpcObserver rpcObserver;
        
        private OpflowRpcObserver.Protocol protocol = OpflowRpcObserver.Protocol.AMQP;
        
        OpflowRpcCheckerMaster(OpflowRestrictor.Valve restrictor, OpflowRpcObserver rpcObserver, OpflowRpcAmqpMaster amqpMaster, OpflowRpcHttpMaster httpMaster) throws OpflowBootstrapException {
            this.restrictor = restrictor;
            this.rpcObserver = rpcObserver;
            this.amqpMaster = amqpMaster;
            this.httpMaster = httpMaster;
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
        
        private OpflowRpcObserver.Protocol next() {
            OpflowRpcObserver.Protocol current = protocol;
            switch (protocol) {
                case AMQP:
                    protocol = OpflowRpcObserver.Protocol.HTTP;
                    break;
                case HTTP:
                    protocol = OpflowRpcObserver.Protocol.AMQP;
                    break;
            }
            return current;
        }
        
        private Pong _send_safe(final Ping ping) throws Throwable {
            OpflowRpcObserver.Protocol proto = next();
            Date startTime = new Date();

            String body = (ping == null) ? DEFAULT_BALL_JSON : OpflowJsonTool.toString(new Object[] { ping });
            String routineId = OpflowUUID.getBase64ID();
            String routineTimestamp = OpflowDateTime.toISO8601UTC(startTime);
            String routineSignature = getSendMethodName();

            Pong pong = null;

            switch (proto) {
                case AMQP:
                    pong = send_over_amqp(routineId, routineTimestamp, routineSignature, body);
                    break;
                case HTTP:
                    pong = send_over_http(routineId, routineTimestamp, routineSignature, body);
                    break;
                default:
                    pong = new Pong();
                    break;
            }

            Date endTime = new Date();

            // updateInfo the observation result
            if (rpcObserver != null) {
                Map<String, Object> serverletInfo = pong.getAccumulator();
                if (serverletInfo != null) {
                    String componentId = serverletInfo.getOrDefault(CONST.COMPONENT_ID, "").toString();
                    if (!componentId.isEmpty()) {
                        if (!rpcObserver.containsInfo(componentId, OpflowConstant.INFO_SECTION_SOURCE_CODE)) {
                            Object serverletCodeRef = serverletInfo.get(OpflowConstant.INFO_SECTION_SOURCE_CODE);
                            if (serverletCodeRef != null) {
                                rpcObserver.updateInfo(componentId, OpflowConstant.INFO_SECTION_SOURCE_CODE, serverletCodeRef);
                            }
                        }
                    }
                }
            }
            // append the context of ping
            pong.getParameters().put(OpflowConstant.ROUTINE_ID, routineId);
            pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_PROTOCOL, proto);
            pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_START_TIMESTAMP, startTime);
            pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_END_TIMESTAMP, endTime);
            pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_ELAPSED_TIME, endTime.getTime() - startTime.getTime());
            return pong;
        }
        
        private Pong send_over_amqp(String routineId, String routineTimestamp, String routineSignature, String body) throws Throwable {
            try {
                OpflowRpcAmqpRequest rpcRequest = amqpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                        .setProgressEnabled(false)
                        .setRoutineScope("internal"));
                OpflowRpcAmqpResult rpcResult = rpcRequest.extractResult(false);

                if (rpcResult.isTimeout()) {
                    throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
                }

                if (rpcResult.isFailed()) {
                    Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcResult.getErrorAsString());
                    throw OpflowUtil.rebuildInvokerException(errorMap);
                }

                rpcObserver.setCongestive(OpflowRpcObserver.Protocol.AMQP, false);

                return OpflowJsonTool.toObject(rpcResult.getValueAsString(), Pong.class);
            }
            catch (Throwable t) {
                rpcObserver.setCongestive(OpflowRpcObserver.Protocol.AMQP, true);
                throw t;
            }
        }
        
        private Pong send_over_http(String routineId, String routineTimestamp, String routineSignature, String body) throws Throwable {
            OpflowRpcRoutingInfo routingInfo = rpcObserver.getRoutingInfo(OpflowRpcObserver.Protocol.HTTP, false);
            if (routingInfo == null) {
                rpcObserver.setCongestive(OpflowRpcObserver.Protocol.HTTP, true);
                throw new OpflowWorkerNotFoundException();
            }
            try {
                OpflowRpcHttpMaster.Session rpcRequest = httpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                        .setProgressEnabled(false)
                        .setRoutineScope("internal"), routingInfo);

                if (rpcRequest.isFailed()) {
                    Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcRequest.getErrorAsString());
                    throw OpflowUtil.rebuildInvokerException(errorMap);
                }
                
                if (rpcRequest.isCracked()) {
                    throw new OpflowRequestFailureException("OpflowRpcChecker.send() call is cracked");
                }

                if (rpcRequest.isTimeout()) {
                    throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
                }
                
                if (!rpcRequest.isOk()) {
                    throw new OpflowRequestFailureException("OpflowRpcChecker.send() is unreasonable");
                }
                
                rpcObserver.setCongestive(OpflowRpcObserver.Protocol.HTTP, false, routingInfo.getComponentId());
                return OpflowJsonTool.toObject(rpcRequest.getValueAsString(), Pong.class);
            }
            catch (Throwable t) {
                rpcObserver.setCongestive(OpflowRpcObserver.Protocol.HTTP, true, routingInfo.getComponentId());
                throw t;
            }
        }
    }

    private static class OpflowTaskSubmitterMaster implements OpflowTaskSubmitter {

        private final String componentId;
        private final OpflowPromMeasurer measurer;
        private final OpflowLogTracer logTracer;
        private final OpflowRestrictorMaster restrictor;
        private final OpflowRpcAmqpMaster amqpMaster;
        private final OpflowRpcHttpMaster httpMaster;
        private final Map<String, RpcInvocationHandler> handlers;
        private final OpflowThroughput.Meter speedMeter;
        
        public OpflowTaskSubmitterMaster(String componentId,
                OpflowPromMeasurer measurer,
                OpflowRestrictorMaster restrictor,
                OpflowRpcAmqpMaster amqpMaster,
                OpflowRpcHttpMaster httpMaster,
                Map<String, RpcInvocationHandler> mappings,
                OpflowThroughput.Meter speedMeter
        ) {
            this.componentId = componentId;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.amqpMaster = amqpMaster;
            this.httpMaster = httpMaster;
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
            if (amqpMaster == null) {
                return OpflowObjectTree.buildMap()
                        .toMap();
            }
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].reset() is invoked")
                    .stringify());
            amqpMaster.reset();
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
            if (amqpMaster != null) {
                amqpMaster.resetCallbackQueueCounter();
            }
            return OpflowObjectTree.buildMap().put("acknowledged", true).toMap();
        }
        
        @Override
        public Map<String, Object> activateRemoteAMQPWorker(boolean state, Map<String, Object> opts) {
            return activateWorker(OpflowConstant.COMP_REMOTE_AMQP_WORKER, state, opts);
        }
        
        @Override
        public Map<String, Object> activateRemoteHTTPWorker(boolean state, Map<String, Object> opts) {
            return activateWorker(OpflowConstant.COMP_REMOTE_HTTP_WORKER, state, opts);
        }
        
        @Override
        public Map<String, Object> activateNativeWorker(boolean state, Map<String, Object> opts) {
            return activateWorker(OpflowConstant.COMP_NATIVE_WORKER, state, opts);
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
            if (OpflowConstant.COMP_REMOTE_AMQP_WORKER.equals(type)) {
                handler.setRemoteAMQPWorkerActive(state);
                return;
            }
            if (OpflowConstant.COMP_REMOTE_HTTP_WORKER.equals(type)) {
                handler.setRemoteHTTPWorkerActive(state);
                return;
            }
            if (OpflowConstant.COMP_NATIVE_WORKER.equals(type)) {
                handler.setNativeWorkerActive(state);
                return;
            }
        }
    }

    private static class OpflowInfoCollectorMaster implements OpflowInfoCollector {
        private final String componentId;
        private final OpflowPromMeasurer measurer;
        private final OpflowRestrictorMaster restrictor;
        private final OpflowRpcWatcher rpcWatcher;
        private final OpflowRpcAmqpMaster amqpMaster;
        private final OpflowRpcHttpMaster httpMaster;
        private final Map<String, RpcInvocationHandler> handlers;
        private final OpflowRpcObserver rpcObserver;
        private final OpflowThroughput.Meter speedMeter;
        private final Date startTime;

        public OpflowInfoCollectorMaster(String componentId,
                OpflowPromMeasurer measurer,
                OpflowRestrictorMaster restrictor,
                OpflowRpcAmqpMaster amqpMaster,
                OpflowRpcHttpMaster httpMaster,
                Map<String, RpcInvocationHandler> mappings,
                OpflowThroughput.Meter speedMeter,
                OpflowRpcObserver rpcObserver,
                OpflowRpcWatcher rpcWatcher
        ) {
            this.componentId = componentId;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.amqpMaster = amqpMaster;
            this.httpMaster = httpMaster;
            this.handlers = mappings;
            this.speedMeter = speedMeter;
            this.rpcObserver = rpcObserver;
            this.rpcWatcher = rpcWatcher;
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
            
            root.put(CONST.INSTANCE_ID, OpflowLogTracer.getInstanceId());
            
            root.put(OpflowConstant.COMP_COMMANDER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put(CONST.COMPONENT_ID, componentId);
                    
                    // RPC AMQP Master information
                    if (amqpMaster != null) {
                        opts.put(OpflowConstant.COMP_RPC_AMQP_MASTER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                OpflowEngine engine = amqpMaster.getEngine();
                                
                                opt2.put(CONST.COMPONENT_ID, amqpMaster.getComponentId());
                                opt2.put(OpflowConstant.OPFLOW_COMMON_APP_ID, engine.getApplicationId());

                                opt2.put(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, engine.getExchangeName());
                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_TYPE, engine.getExchangeType());
                                    opt2.put(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_DURABLE, engine.getExchangeDurable());
                                }
                                opt2.put(OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY, engine.getRoutingKey());

                                opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME, amqpMaster.getResponseQueueName());
                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_AUTO_DELETE, amqpMaster.getResponseQueueAutoDelete());
                                    opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_DURABLE, amqpMaster.getResponseQueueDurable());
                                    opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_EXCLUSIVE, amqpMaster.getResponseQueueExclusive());
                                }

                                opt2.put("request", OpflowObjectTree.buildMap()
                                        .put(OpflowConstant.AMQP_PARAM_MESSAGE_TTL, amqpMaster.getExpiration())
                                        .toMap());

                                if (checkOption(flag, SCOPE_INFO)) {
                                    opt2.put("transport", CONST.getProtocolInfo());
                                }
                            }
                        }).toMap());
                    }
                    
                    // RPC HTTP Master information
                    if (httpMaster != null) {
                        opts.put(OpflowConstant.COMP_RPC_HTTP_MASTER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                opt2.put(CONST.COMPONENT_ID, httpMaster.getComponentId());
                                opt2.put("request", OpflowObjectTree.buildMap()
                                        .put(OpflowConstant.HTTP_MASTER_PARAM_PULL_TIMEOUT, httpMaster.getReadTimeout())
                                        .put(OpflowConstant.HTTP_MASTER_PARAM_PUSH_TIMEOUT, httpMaster.getWriteTimeout())
                                        .put(OpflowConstant.HTTP_MASTER_PARAM_CALL_TIMEOUT, httpMaster.getCallTimeout())
                                        .toMap());
                            }
                        }).toMap());
                    }
                    
                    // RPC mappings
                    if (checkOption(flag, SCOPE_INFO)) {
                        opts.put("mappings", renderRpcInvocationHandlers(handlers));
                    }
                    
                    // RpcWatcher information
                    if (checkOption(flag, SCOPE_INFO)) {
                        opts.put(OpflowConstant.COMP_RPC_WATCHER, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COMMON_ENABLED, rpcWatcher.isEnabled())
                                .put(OpflowConstant.OPFLOW_COMMON_INTERVAL, rpcWatcher.getInterval())
                                .put(OpflowConstant.OPFLOW_COMMON_COUNT, rpcWatcher.getCount())
                                .toMap());
                    }
                    
                    // restrictor information
                    if (checkOption(flag, SCOPE_INFO)) {
                        if (restrictor != null) {
                            opts.put(OpflowConstant.COMP_RESTRICTOR, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                                @Override
                                public void transform(Map<String, Object> opt2) {
                                    int availablePermits = restrictor.getSemaphorePermits();
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_ENABLED, restrictor.isPauseEnabled());
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_TIMEOUT, restrictor.getPauseTimeout());
                                    if (restrictor.isPaused()) {
                                        opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_STATUS, "on");
                                        opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_ELAPSED_TIME, restrictor.getPauseElapsed());
                                        opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_DURATION, restrictor.getPauseDuration());
                                    } else {
                                        opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_STATUS, "off");
                                    }
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_PERMITS, restrictor.getSemaphoreLimit());
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_USED_PERMITS, restrictor.getSemaphoreLimit() - availablePermits);
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_FREE_PERMITS, availablePermits);
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_ENABLED, restrictor.isSemaphoreEnabled());
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_TIMEOUT, restrictor.getSemaphoreTimeout());
                                }
                            }).toMap());
                        } else {
                            opts.put(OpflowConstant.COMP_RESTRICTOR, OpflowObjectTree.buildMap()
                                    .put(OpflowConstant.OPFLOW_COMMON_ENABLED, false)
                                    .toMap());
                        }
                    }
                    
                    // start-time & uptime
                    if (checkOption(flag, SCOPE_INFO)) {
                        Date currentTime = new Date();
                        opts.put(OpflowConstant.INFO_SECTION_RUNTIME, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COMMON_CONGESTIVE, rpcObserver.isCongestive())
                                .put("threadCount", Thread.activeCount())
                                .put(OpflowConstant.OPFLOW_COMMON_START_TIMESTAMP, startTime)
                                .put("currentTime", currentTime)
                                .put("uptime", OpflowDateTime.printElapsedTime(startTime, currentTime))
                                .toMap());
                    }
                    
                    // git commit information
                    if (checkOption(flag, SCOPE_INFO)) {
                        opts.put(OpflowConstant.INFO_SECTION_SOURCE_CODE, OpflowObjectTree.buildMap()
                                .put("server", OpflowSysInfo.getGitInfo("META-INF/scm/service-master/git-info.json"))
                                .put(CONST.FRAMEWORK_ID, OpflowSysInfo.getGitInfo())
                                .toMap());
                    }
                }
            }).toMap());

            // current serverlets
            if (checkOption(flag, SCOPE_INFO)) {
                if (rpcObserver != null) {
                    root.put(OpflowConstant.COMP_SERVERLET, rpcObserver.summary());
                }
            }

            return root.toMap();
        }
        
        @Override
        public Map<String, Object> traffic(Map<String, Boolean> options) {
            final Map<String, Boolean> flag = (options != null) ? options : new HashMap<String, Boolean>();
            
            Map<String, Object> metrics = OpflowObjectTree.buildMap().toMap();
            
            // update the RPC invocation counters
            if (measurer != null) {
                OpflowPromMeasurer.RpcInvocationCounter counter = measurer.getRpcInvocationCounter(OpflowConstant.COMP_COMMANDER);
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
                        .put(OpflowPromMeasurer.LABEL_RPC_REMOTE_AMQP_WORKER, OpflowObjectTree.buildMap()
                                .put("waitingReqTotal", OpflowObjectTree.buildMap()
                                        .put("current", amqpMaster.getActiveRequestTotal())
                                        .put("top", amqpMaster.getMaxWaitingRequests())
                                        .toMap())
                                .toMap())
                        .toMap());
            } else {
                Map<String, Object> parentOfQueueInfo;
                Object remoteAmqpWorkerInfo = metrics.get(OpflowPromMeasurer.LABEL_RPC_REMOTE_AMQP_WORKER);
                if (remoteAmqpWorkerInfo instanceof Map) {
                    parentOfQueueInfo = (Map<String, Object>) remoteAmqpWorkerInfo;
                } else {
                    parentOfQueueInfo = OpflowObjectTree.buildMap().toMap();
                    metrics.put(OpflowPromMeasurer.LABEL_RPC_REMOTE_AMQP_WORKER, parentOfQueueInfo);
                }
                parentOfQueueInfo.put("waitingReqTotal", OpflowObjectTree.buildMap()
                        .put("current", amqpMaster.getActiveRequestTotal())
                        .put("top", amqpMaster.getMaxWaitingRequests())
                        .toMap());
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
                        if (val.getNativeWorkerClassName() != null) {
                            opts.put("nativeWorkerClassName", val.getNativeWorkerClassName());
                        }
                        opts.put("nativeWorkerActive", val.isNativeWorkerActive());
                        opts.put("nativeWorkerAvailable", val.isNativeWorkerAvailable());
                        opts.put("amqpWorkerActive", val.isRemoteAMQPWorkerActive());
                        opts.put("amqpWorkerAvailable", val.isRemoteAMQPWorkerAvailable());
                        opts.put("httpWorkerActive", val.isRemoteHTTPWorkerActive());
                        opts.put("httpWorkerAvailable", val.isRemoteHTTPWorkerAvailable());
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
        private final OpflowRpcObserver rpcObserver;
        
        private final OpflowRpcAmqpMaster amqpMaster;
        private final OpflowRpcHttpMaster httpMaster;
        private final OpflowPubsubHandler publisher;
        
        private final Class clazz;
        private final Object nativeWorker;
        private final boolean nativeWorkerEnabled;
        private boolean nativeWorkerActive = true;
        private final Map<String, String> aliasOfMethod = new HashMap<>();
        private final Map<String, Boolean> methodIsAsync = new HashMap<>();

        private boolean remoteAMQPWorkerActive = true;
        private boolean remoteHTTPWorkerActive = false;
        
        private final int[] masterFlags;
        
        public RpcInvocationHandler(
            OpflowLogTracer logTracer,
            OpflowPromMeasurer measurer,
            OpflowRestrictorMaster restrictor,
            OpflowReqExtractor reqExtractor,
            OpflowRpcObserver rpcObserver,
            OpflowRpcAmqpMaster amqpMaster,
            OpflowRpcHttpMaster httpMaster,
            OpflowPubsubHandler publisher,
            Class clazz,
            Object nativeWorker,
            boolean nativeWorkerEnabled
        ) {
            this.logTracer = logTracer;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.reqExtractor = reqExtractor;
            this.rpcObserver = rpcObserver;
            
            this.amqpMaster = amqpMaster;
            this.httpMaster = httpMaster;
            this.publisher = publisher;
            
            this.masterFlags = new int[] { FLAG_AMQP, FLAG_HTTP };
            
            this.clazz = clazz;
            this.nativeWorker = nativeWorker;
            this.nativeWorkerEnabled = nativeWorkerEnabled;

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

        public boolean isNativeWorkerActive() {
            return nativeWorkerActive;
        }

        public void setNativeWorkerActive(boolean nativeWorkerActive) {
            this.nativeWorkerActive = nativeWorkerActive;
        }

        public boolean isNativeWorkerAvailable() {
            return this.nativeWorker != null && this.nativeWorkerEnabled && this.nativeWorkerActive;
        }

        public String getNativeWorkerClassName() {
            if (this.nativeWorker == null) return null;
            return this.nativeWorker.getClass().getName();
        }
        
        public Integer getNativeWorkerHashCode() {
            if (this.nativeWorker == null) return null;
            return this.nativeWorker.hashCode();
        }
        
        public boolean isRemoteAMQPWorkerActive() {
            return this.remoteAMQPWorkerActive;
        }
        
        public void setRemoteAMQPWorkerActive(boolean remoteWorkerActive) {
            this.remoteAMQPWorkerActive = remoteWorkerActive;
        }
        
        public boolean isRemoteAMQPWorkerAvailable() {
            return !rpcObserver.isCongestive(OpflowRpcObserver.Protocol.AMQP) && isRemoteAMQPWorkerActive();
        }
        
        public boolean isRemoteHTTPWorkerActive() {
            return this.remoteHTTPWorkerActive;
        }
        
        public void setRemoteHTTPWorkerActive(boolean remoteWorkerActive) {
            this.remoteHTTPWorkerActive = remoteWorkerActive;
        }
        
        public boolean isRemoteHTTPWorkerAvailable() {
            return !rpcObserver.isCongestive(OpflowRpcObserver.Protocol.HTTP) && isRemoteHTTPWorkerActive();
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
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-publish-method] - RpcInvocationHandler.invoke() dispatch the call to the publisher")
                        .stringify());
                measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_FLOW_PUBSUB, routineSignature, "begin");
                this.publisher.publish(body, OpflowObjectTree.buildMap(false)
                        .put(CONST.AMQP_HEADER_ROUTINE_ID, routineId)
                        .put(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, routineTimestamp)
                        .put(CONST.AMQP_HEADER_ROUTINE_SIGNATURE, routineSignature)
                        .toMap());
                return null;
            } else {
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-dispatch-method] - RpcInvocationHandler.invoke() dispatch the call to the RPC Master")
                        .stringify());
                measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_FLOW_RPC, routineSignature, "begin");
            }
            
            if (!isNativeWorkerAvailable() && !isRemoteAMQPWorkerActive() && !isRemoteHTTPWorkerActive()) {
                throw new OpflowWorkerNotFoundException("all of workers are deactivated");
            }
            
            boolean unfinished = true;
            
            for (int flag : masterFlags) {
                if ((flag & FLAG_AMQP) != FLAG_AMQP) {
                    if (isRemoteAMQPWorkerAvailable()) {
                        unfinished = false;

                        OpflowRpcAmqpRequest amqpSession = amqpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                                .setProgressEnabled(false));
                        OpflowRpcAmqpResult amqpResult = amqpSession.extractResult(false);

                        if (amqpResult.isCompleted()) {
                            if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                                    .put("returnType", method.getReturnType().getName())
                                    .put("returnValue", amqpResult.getValueAsString())
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-amqp-worker-ok] - RpcInvocationHandler.invoke() return the output")
                                    .stringify());

                            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "ok");

                            if (method.getReturnType() == void.class) return null;

                            return OpflowJsonTool.toObject(amqpResult.getValueAsString(), method.getReturnType());
                        }

                        if (amqpResult.isFailed()) {
                            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "failed");
                            if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-amqp-worker-failed] - RpcInvocationHandler.invoke() has failed")
                                    .stringify());
                            Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(amqpResult.getErrorAsString());
                            throw OpflowUtil.rebuildInvokerException(errorMap);
                        }

                        if (amqpResult.isTimeout()) {
                            if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-amqp-worker-timeout] - RpcInvocationHandler.invoke() is timeout")
                                    .stringify());
                        }

                        unfinished = true;
                        rpcObserver.setCongestive(OpflowRpcObserver.Protocol.AMQP, true);
                    }
                }

                if ((flag & FLAG_HTTP) != FLAG_HTTP) {
                    OpflowRpcRoutingInfo routingInfo = rpcObserver.getRoutingInfo(OpflowRpcObserver.Protocol.HTTP);
                    if (isRemoteHTTPWorkerAvailable() && routingInfo != null) {
                        unfinished = false;

                        OpflowRpcHttpMaster.Session httpSession = httpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                                .setProgressEnabled(false), routingInfo);

                        if (httpSession.isOk()) {
                            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_HTTP_WORKER, routineSignature, "ok");
                            if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                                    .put("returnType", method.getReturnType().getName())
                                    .put("returnValue", httpSession.getValueAsString())
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-ok] - RpcInvocationHandler.invoke() return the output")
                                    .stringify());
                            if (method.getReturnType() == void.class) return null;
                            return OpflowJsonTool.toObject(httpSession.getValueAsString(), method.getReturnType());
                        }

                        if (httpSession.isFailed()) {
                            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_HTTP_WORKER, routineSignature, "failed");
                            if (reqTracer.ready(LOG, Level.DEBUG)) LOG.debug(reqTracer
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-failed] - RpcInvocationHandler.invoke() has failed")
                                    .stringify());
                            Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(httpSession.getErrorAsString());
                            throw OpflowUtil.rebuildInvokerException(errorMap);
                        }

                        if (httpSession.isTimeout()) {
                            if (reqTracer.ready(LOG, Level.DEBUG)) {
                                LOG.debug(reqTracer
                                        .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-timeout] - RpcInvocationHandler.invoke() is timeout")
                                        .stringify());
                            }
                        }

                        if (httpSession.isCracked()) {
                            if (reqTracer.ready(LOG, Level.DEBUG)) {
                                LOG.debug(reqTracer
                                        .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-cracked] - RpcInvocationHandler.invoke() is cracked")
                                        .stringify());
                            }
                        }

                        unfinished = true;
                        rpcObserver.setCongestive(OpflowRpcObserver.Protocol.HTTP, true, routingInfo.getComponentId());
                    }
                }
            }
            
            if (isNativeWorkerAvailable()) {
                if (unfinished) {
                    if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                            .text("Request[${requestId}][${requestTime}][x-commander-native-worker-rescue] - RpcInvocationHandler.invoke() rescues by the nativeWorker")
                            .stringify());
                    measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER, routineSignature, "rescue");
                } else {
                    if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                            .text("Request[${requestId}][${requestTime}][x-commander-native-worker-retain] - RpcInvocationHandler.invoke() retains the nativeWorker")
                            .stringify());
                    measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER, routineSignature, "retain");
                }
                return method.invoke(this.nativeWorker, args);
            } else {
                if (reqTracer.ready(LOG, Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-remote-all-workers-timeout] - RpcInvocationHandler.invoke() is timeout")
                        .stringify());
                measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "timeout");
                throw new OpflowRequestTimeoutException();
            }
        }
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
            handlers.put(clazzName, new RpcInvocationHandler(logTracer, measurer, restrictor, reqExtractor, rpcObserver, 
                    amqpMaster, httpMaster, publisher, clazz, bean, nativeWorkerEnabled));
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
        return measurer.getRpcInvocationCounter(OpflowConstant.COMP_COMMANDER).toMap();
    }

    public void resetRpcInvocationCounter() {
        measurer.resetRpcInvocationCounter();
    }

    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
