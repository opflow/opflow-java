package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.OpflowUtil.MapBuilder;
import com.devebot.opflow.annotation.OpflowSourceRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import com.devebot.opflow.supports.OpflowDateTime;
import io.undertow.server.RoutingHandler;
import java.lang.annotation.Annotation;
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
    public final static List<String> SERVICE_BEAN_NAMES = Arrays.asList(new String[] {
        "configurer", "rpcMaster", "publisher"
    });

    public final static List<String> SUPPORT_BEAN_NAMES = Arrays.asList(new String[] {
        "restrictor", "rpcWatcher", "promExporter", "restServer"
    });

    public final static List<String> ALL_BEAN_NAMES = OpflowUtil.mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);

    public static enum RestrictionScope { ALL, RPC }
    
    public final static String PARAM_RESERVE_WORKER_ENABLED = "reserveWorkerEnabled";

    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommander.class);

    private final String commanderId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowConfig.Loader configLoader;

    private RestrictionScope restrictorScope = RestrictionScope.ALL;
    private OpflowRestrictor restrictor;
    
    private boolean reserveWorkerEnabled;
    private OpflowPubsubHandler configurer;
    private OpflowRpcMaster rpcMaster;
    private OpflowPubsubHandler publisher;
    private OpflowRpcChecker rpcChecker;
    private OpflowRpcWatcher rpcWatcher;
    private OpflowRestServer restServer;

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
        commanderId = OpflowUtil.getOptionField(kwargs, "commanderId", true);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", commanderId);

        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].new()")
                .stringify());

        measurer = OpflowPromMeasurer.getInstance();
        
        this.init(kwargs);

        measurer.changeComponentInstance("commander", commanderId, OpflowPromMeasurer.GaugeAction.INC);

        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].new() end!")
                .stringify());
    }
    
    private void init(Map<String, Object> kwargs) throws OpflowBootstrapException {
        if (kwargs.get(PARAM_RESERVE_WORKER_ENABLED) != null && kwargs.get(PARAM_RESERVE_WORKER_ENABLED) instanceof Boolean) {
            reserveWorkerEnabled = (Boolean) kwargs.get(PARAM_RESERVE_WORKER_ENABLED);
        } else {
            reserveWorkerEnabled = true;
        }

        Map<String, Object> restrictorCfg = (Map<String, Object>)kwargs.get("restrictor");
        Map<String, Object> configurerCfg = (Map<String, Object>)kwargs.get("configurer");
        Map<String, Object> rpcMasterCfg = (Map<String, Object>)kwargs.get("rpcMaster");
        Map<String, Object> publisherCfg = (Map<String, Object>)kwargs.get("publisher");
        Map<String, Object> rpcWatcherCfg = (Map<String, Object>)kwargs.get("rpcWatcher");
        Map<String, Object> restServerCfg = (Map<String, Object>)kwargs.get("restServer");

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
            if (OpflowUtil.isComponentEnabled(restrictorCfg)) {
                restrictor = new OpflowRestrictor(OpflowUtil.buildMap(restrictorCfg)
                        .put("instanceId", commanderId)
                        .put("pauseTimeout", OpflowUtil.getOptionField(restrictorCfg, "suspendTimeout", null))
                        .toMap());
            }

            if (OpflowUtil.isComponentEnabled(configurerCfg)) {
                configurer = new OpflowPubsubHandler(OpflowUtil.buildMap(new OpflowUtil.MapListener() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("measurer", measurer);
                    }
                }, configurerCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(rpcMasterCfg)) {
                rpcMaster = new OpflowRpcMaster(OpflowUtil.buildMap(new OpflowUtil.MapListener() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("measurer", measurer);
                        if (restrictorScope == RestrictionScope.RPC) {
                            opts.put("restrictor", restrictor);
                        }
                    }
                }, rpcMasterCfg).toMap());
            }
            if (OpflowUtil.isComponentEnabled(publisherCfg)) {
                publisher = new OpflowPubsubHandler(OpflowUtil.buildMap(new OpflowUtil.MapListener() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("measurer", measurer);
                    }
                }, publisherCfg).toMap());
            }

            rpcChecker = new OpflowRpcCheckerMaster(rpcMaster);

            rpcWatcher = new OpflowRpcWatcher(rpcChecker, OpflowUtil.buildMap(rpcWatcherCfg)
                    .put("instanceId", commanderId)
                    .toMap());

            rpcWatcher.start();

            OpflowInfoCollector infoCollector = new OpflowInfoCollectorMaster(commanderId, measurer, restrictor, rpcWatcher, rpcMaster, handlers);

            OpflowTaskSubmitter taskSubmitter = new OpflowTaskSubmitterMaster(commanderId, restrictor, rpcMaster, handlers);
            
            restServer = new OpflowRestServer(infoCollector, taskSubmitter, rpcChecker, OpflowUtil.buildMap(restServerCfg)
                    .put("instanceId", commanderId)
                    .toMap());
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
    }
    
    public boolean isRestrictorAvailable() {
        return restrictor != null && restrictorScope == RestrictionScope.ALL;
    }
    
    public boolean isReserveWorkerEnabled() {
        return this.reserveWorkerEnabled;
    }
    
    public void setReserveWorkerEnabled(boolean enabled) {
        this.reserveWorkerEnabled = enabled;
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
        if (restServer != null) {
            restServer.serve();
        }
    }

    public final void serve(RoutingHandler httpHandlers) {
        if (restServer != null) {
            restServer.serve(httpHandlers);
        }
    }

    @Override
    public final void close() {
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].close()")
                .stringify());

        if (restServer != null) restServer.close();
        if (rpcWatcher != null) rpcWatcher.close();
        
        if (isRestrictorAvailable()) {
            restrictor.lock();
        }
        try {
            if (publisher != null) publisher.close();
            if (rpcMaster != null) rpcMaster.close();
            if (configurer != null) configurer.close();
        }
        finally {
            if (isRestrictorAvailable()) {
                restrictor.unlock();
            }
        }
        
        if (isRestrictorAvailable()) {
            restrictor.close();
        }

        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].close() has done!")
                .stringify());
    }

    private static class OpflowRpcCheckerMaster extends OpflowRpcChecker {

        private final OpflowRpcMaster rpcMaster;

        OpflowRpcCheckerMaster(OpflowRpcMaster rpcMaster) throws OpflowBootstrapException {
            this.rpcMaster = rpcMaster;
        }

        @Override
        public Pong send(Ping ping) throws Throwable {
            String body = OpflowJsonTool.toString(new Object[] { ping });
            String requestId = OpflowUtil.getLogID();
            Date startTime = new Date();
            OpflowRpcRequest rpcRequest = rpcMaster.request(getSendMethodName(), body, OpflowUtil.buildMap()
                    .put("requestId", requestId)
                    .put("messageScope", "internal")
                    .put("progressEnabled", false)
                    .toMap());
            Date endTime = new Date();

            OpflowRpcResult rpcResult = rpcRequest.extractResult(false);

            if (rpcResult.isTimeout()) {
                throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
            }

            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            Pong pong = OpflowJsonTool.toObject(rpcResult.getValueAsString(), Pong.class);
            pong.getParameters().put("requestId", requestId);
            pong.getParameters().put("startTime", OpflowUtil.toISO8601UTC(startTime));
            pong.getParameters().put("endTime", OpflowUtil.toISO8601UTC(endTime));
            pong.getParameters().put("elapsedTime", endTime.getTime() - startTime.getTime());
            return pong;
        }
    }

    private static class OpflowTaskSubmitterMaster implements OpflowTaskSubmitter {

        private final String instanceId;
        private final OpflowLogTracer logTracer;
        private final OpflowRestrictor restrictor;
        private final OpflowRpcMaster rpcMaster;
        private final Map<String, RpcInvocationHandler> handlers;

        public OpflowTaskSubmitterMaster(String instanceId,
                OpflowRestrictor restrictor,
                OpflowRpcMaster rpcMaster,
                Map<String, RpcInvocationHandler> mappings) {
            this.instanceId = instanceId;
            this.restrictor = restrictor;
            this.rpcMaster = rpcMaster;
            this.handlers = mappings;
            this.logTracer = OpflowLogTracer.ROOT.branch("taskSubmitterId", instanceId);
        }
        
        @Override
        public Map<String, Object> pause(long duration) {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].pause(true) is invoked")
                    .stringify());
            if (restrictor == null) {
                return OpflowUtil.buildOrderedMap()
                        .toMap();
            }
            return restrictor.pause(duration);
        }
        
        @Override
        public Map<String, Object> unpause() {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].unpause() is invoked")
                    .stringify());
            if (restrictor == null) {
                return OpflowUtil.buildOrderedMap()
                        .toMap();
            }
            return restrictor.unpause();
        }
        
        @Override
        public Map<String, Object> reset() {
            if (rpcMaster == null) {
                return OpflowUtil.buildOrderedMap()
                        .toMap();
            }
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].reset() is invoked")
                    .stringify());
            rpcMaster.close();
            return OpflowUtil.buildOrderedMap()
                    .toMap();
        }

        @Override
        public Map<String, Object> state(Map<String, Object> opts) {
            String clazz = (String) OpflowUtil.getOptionField(opts, "type", null);
            Boolean status = (Boolean) OpflowUtil.getOptionField(opts, "status", Boolean.TRUE);
            for(final Map.Entry<String, RpcInvocationHandler> entry : handlers.entrySet()) {
                final String key = entry.getKey();
                final RpcInvocationHandler val = entry.getValue();
                if (clazz != null) {
                    if (clazz.equals(key)) {
                        val.setReserveWorkerForced(status);
                        break;
                    }
                } else {
                    val.setReserveWorkerForced(status);
                }
            }
            return OpflowUtil.buildOrderedMap()
                    .put("mappings", OpflowInfoCollectorMaster.renderRpcInvocationHandlers(handlers))
                    .toMap();
        }
    }
    
    private static class OpflowInfoCollectorMaster implements OpflowInfoCollector {

        private final String instanceId;
        private final OpflowPromMeasurer measurer;
        private final OpflowRestrictor restrictor;
        private final OpflowRpcWatcher rpcWatcher;
        private final OpflowRpcMaster rpcMaster;
        private final Map<String, RpcInvocationHandler> handlers;
        private final Date startTime;

        public OpflowInfoCollectorMaster(String instanceId,
                OpflowPromMeasurer measurer,
                OpflowRestrictor restrictor,
                OpflowRpcWatcher rpcWatcher,
                OpflowRpcMaster rpcMaster,
                Map<String, RpcInvocationHandler> mappings) {
            this.instanceId = instanceId;
            this.measurer = measurer;
            this.restrictor = restrictor;
            this.rpcWatcher = rpcWatcher;
            this.rpcMaster = rpcMaster;
            this.handlers = mappings;
            this.startTime = new Date();
        }

        @Override
        public Map<String, Object> collect() {
            return collect(null);
        }

        @Override
        public Map<String, Object> collect(Scope scope) {
            final Scope label = (scope == null) ? Scope.BASIC : scope;
            
            MapBuilder root = OpflowUtil.buildOrderedMap();
            
            root.put("commander", OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put("instanceId", instanceId);

                    // restrictor information
                    if (label == Scope.FULL) {
                        if (restrictor != null) {
                            opts.put("restrictor", OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
                                @Override
                                public void transform(Map<String, Object> opt2) {
                                    int availablePermits = restrictor.getSemaphorePermits();
                                    opt2.put("semaphoreLimit", restrictor.getSemaphoreLimit());
                                    opt2.put("semaphoreUsedPermits", restrictor.getSemaphoreLimit() - availablePermits);
                                    opt2.put("semaphoreFreePermits", availablePermits);
                                    opt2.put("semaphoreEnabled", restrictor.isSemaphoreEnabled());
                                    opt2.put("semaphoreTimeout", restrictor.getSemaphoreTimeout());
                                    opt2.put("pauseEnabled", restrictor.isPauseEnabled());
                                    opt2.put("pauseTimeout", restrictor.getPauseTimeout());
                                    opt2.put("pauseStatus", restrictor.isPaused() ? "on" : "off");
                                }
                            }).toMap());
                        } else {
                            opts.put("restrictor", OpflowUtil.buildOrderedMap()
                                    .put("enabled", false)
                                    .toMap());
                        }
                    }
                    
                    // rpcMaster information
                    if (rpcMaster != null) {
                        opts.put("rpcMaster", OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                OpflowEngine engine = rpcMaster.getEngine();
                                
                                opt2.put("instanceId", rpcMaster.getInstanceId());
                                opt2.put("applicationId", engine.getApplicationId());
                                opt2.put("exchangeName", engine.getExchangeName());

                                if (label == Scope.FULL) {
                                    opt2.put("exchangeDurable", engine.getExchangeDurable());
                                }

                                opt2.put("routingKey", engine.getRoutingKey());

                                if (label == Scope.FULL) {
                                    opt2.put("otherKeys", engine.getOtherKeys());
                                }

                                opt2.put("callbackQueue", rpcMaster.getCallbackName());

                                if (label == Scope.FULL) {
                                    opt2.put("callbackDurable", rpcMaster.getCallbackDurable());
                                    opt2.put("callbackExclusive", rpcMaster.getCallbackExclusive());
                                    opt2.put("callbackAutoDelete", rpcMaster.getCallbackAutoDelete());
                                }

                                opt2.put("request", OpflowUtil.buildOrderedMap()
                                        .put("expiration", rpcMaster.getExpiration())
                                        .toMap());
                            }
                        }).toMap());
                    }
                    
                    // RPC mappings
                    if (label == Scope.FULL) {
                        opts.put("mappings", renderRpcInvocationHandlers(handlers));
                    }
                    
                    // RpcWatcher information
                    if (label == Scope.FULL) {
                        opts.put("rpcWatcher", OpflowUtil.buildOrderedMap()
                                .put("enabled", rpcWatcher.isEnabled())
                                .put("interval", rpcWatcher.getInterval())
                                .put("count", rpcWatcher.getCount())
                                .put("congested", rpcWatcher.isCongested())
                                .toMap());
                    }
                }
            }).toMap());

            // start-time & uptime
            if (label == Scope.FULL) {
                Date currentTime = new Date();
                root.put("miscellaneous", OpflowUtil.buildOrderedMap()
                        .put("threadCount", Thread.activeCount())
                        .put("startTime", OpflowUtil.toISO8601UTC(startTime))
                        .put("currentTime", OpflowUtil.toISO8601UTC(currentTime))
                        .put("uptime", OpflowDateTime.printElapsedTime(startTime, currentTime))
                        .toMap());
            }
            
            return root.toMap();
        }
        
        protected static List<Map<String, Object>> renderRpcInvocationHandlers(Map<String, RpcInvocationHandler> handlers) {
            List<Map<String, Object>> mappingInfos = new ArrayList<>();
            for(final Map.Entry<String, RpcInvocationHandler> entry : handlers.entrySet()) {
                final RpcInvocationHandler val = entry.getValue();
                mappingInfos.add(OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("class", entry.getKey());
                        opts.put("methods", val.getMethodNames());
                        opts.put("isReserveWorkerReady", val.hasReserveWorker());
                        opts.put("forceUseReserveWorker", val.isReserveWorkerForced());
                        if (val.getReserveWorkerClassName() != null) {
                            opts.put("reserveWorkerClassName", val.getReserveWorkerClassName());
                        }
                    }
                }).toMap());
            }
            return mappingInfos;
        }
    }

    private class RpcInvocationHandler implements InvocationHandler {
        private final Class clazz;
        private final Object reserveWorker;
        private final boolean reserveWorkerEnabled;
        private final Map<String, String> aliasOfMethod = new HashMap<>();
        private final Map<String, Boolean> methodIsAsync = new HashMap<>();
        private final OpflowRpcMaster rpcMaster;
        private final OpflowPubsubHandler publisher;
        
        private boolean reserveWorkerForced = false;

        public boolean isReserveWorkerForced() {
            return reserveWorkerForced;
        }
        
        public void setReserveWorkerForced(boolean reserveWorkerForced) {
            this.reserveWorkerForced = reserveWorkerForced;
        }
        
        public RpcInvocationHandler(OpflowRpcMaster rpcMaster, OpflowPubsubHandler publisher, Class clazz, Object reserveWorker, boolean reserveWorkerEnabled) {
            this.clazz = clazz;
            this.reserveWorker = reserveWorker;
            this.reserveWorkerEnabled = reserveWorkerEnabled;
            for (Method method : this.clazz.getDeclaredMethods()) {
                String methodId = OpflowUtil.getMethodSignature(method);
                OpflowSourceRoutine routine = extractMethodInfo(method);
                if (routine != null && routine.alias() != null && routine.alias().length() > 0) {
                    String alias = routine.alias();
                    if (aliasOfMethod.containsValue(alias)) {
                        throw new OpflowInterceptionException("Alias[" + alias + "]/routineId[" + methodId + "] is duplicated");
                    }
                    aliasOfMethod.put(methodId, alias);
                    if (logTracer.ready(LOG, "trace")) LOG.trace(logTracer
                            .put("alias", alias)
                            .put("routineId", methodId)
                            .text("link alias to routineId")
                            .stringify());
                }
                methodIsAsync.put(methodId, (routine != null) && routine.isAsync());
            }
            this.rpcMaster = rpcMaster;
            this.publisher = publisher;
        }

        public boolean hasReserveWorker() {
            return this.reserveWorker != null && this.reserveWorkerEnabled;
        }

        public String getReserveWorkerClassName() {
            if (this.reserveWorker == null) return null;
            return this.reserveWorker.getClass().getName();
        }
        
        public Set<String> getMethodNames() {
            return methodIsAsync.keySet();
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            if (isRestrictorAvailable()) {
                return _invoke(proxy, method, args);
            }
            return restrictor.filter(new OpflowRestrictor.Action<Object>() {
                @Override
                public Object process() throws Throwable {
                    return _invoke(proxy, method, args);
                }
            });
        }
        
        private Object _invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final String requestId = OpflowUtil.getLogID();
            final OpflowLogTracer logRequest = logTracer.branch("requestId", requestId);
            String methodId = OpflowUtil.getMethodSignature(method);
            String routineId = aliasOfMethod.getOrDefault(methodId, methodId);
            Boolean isAsync = methodIsAsync.getOrDefault(methodId, false);
            if (logRequest.ready(LOG, "info")) LOG.info(logRequest
                    .put("methodId", methodId)
                    .put("routineId", routineId)
                    .put("isAsync", isAsync)
                    .text("Request[${requestId}] - RpcInvocationHandler.invoke() - method[${routineId}] is async: ${isAsync}")
                    .stringify());

            if (args == null) args = new Object[0];
            String body = OpflowJsonTool.toString(args);

            if (logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                    .put("args", args)
                    .put("body", body)
                    .text("Request[${requestId}] - RpcInvocationHandler.invoke() details")
                    .stringify());

            if (this.publisher != null && isAsync && void.class.equals(method.getReturnType())) {
                if (logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                        .text("Request[${requestId}] - RpcInvocationHandler.invoke() dispatch the call to the publisher")
                        .stringify());
                this.publisher.publish(body, OpflowUtil.buildMap()
                        .put("requestId", requestId)
                        .put("routineId", routineId)
                        .toMap());
                return null;
            } else {
                if (logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                        .text("Request[${requestId}] - RpcInvocationHandler.invoke() dispatch the call to the rpcMaster")
                        .stringify());
            }

            // rpc switching
            if (rpcWatcher.isCongested() || reserveWorkerForced) {
                if (this.hasReserveWorker()) {
                    return method.invoke(this.reserveWorker, args);
                }
            }

            OpflowRpcRequest rpcSession = rpcMaster.request(routineId, body, OpflowUtil.buildMap()
                    .put("requestId", requestId)
                    .put("progressEnabled", false)
                    .toMap());
            OpflowRpcResult rpcResult = rpcSession.extractResult(false);

            if (rpcResult.isTimeout()) {
                rpcWatcher.setCongested(true);
                if (this.hasReserveWorker()) {
                    return method.invoke(this.reserveWorker, args);
                }
                throw new OpflowRequestTimeoutException();
            }

            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            if (logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                    .put("returnType", method.getReturnType().getName())
                    .put("returnValue", rpcResult.getValueAsString())
                    .text("Request[${requestId}] - RpcInvocationHandler.invoke() return the output")
                    .stringify());

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

    private RpcInvocationHandler getInvocationHandler(Class clazz, Object bean) {
        validateType(clazz);
        String clazzName = clazz.getName();
        if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
                .put("className", clazzName)
                .text("getInvocationHandler() get InvocationHandler by type")
                .stringify());
        if (!handlers.containsKey(clazzName)) {
            if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
                    .put("className", clazzName)
                    .text("getInvocationHandler() InvocationHandler not found, create new one")
                    .stringify());
            handlers.put(clazzName, new RpcInvocationHandler(rpcMaster, publisher, clazz, bean, reserveWorkerEnabled));
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
            if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
                    .put("typeString", type.toGenericString())
                    .text("generic types are unsupported")
                    .stringify());
        }
        Method[] methods = type.getDeclaredMethods();
        for(Method method:methods) {
            if (OpflowUtil.isGenericDeclaration(method.toGenericString())) {
                ok = false;
                if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
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
            if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
                    .put("className", type.getName())
                    .put("classLoaderName", type.getClassLoader().getClass().getName())
                    .text("registerType() calls newProxyInstance()")
                    .stringify());
            T t = (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type, bean));
            if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
                    .put("className", type.getName())
                    .text("newProxyInstance() has completed")
                    .stringify());
            return t;
        } catch (IllegalArgumentException exception) {
            if (logTracer.ready(LOG, "error")) LOG.error(logTracer
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

    private OpflowSourceRoutine extractMethodInfo(Method method) {
        if (!method.isAnnotationPresent(OpflowSourceRoutine.class)) return null;
        Annotation annotation = method.getAnnotation(OpflowSourceRoutine.class);
        OpflowSourceRoutine routine = (OpflowSourceRoutine) annotation;
        return routine;
    }

    @Override
    protected void finalize() throws Throwable {
        measurer.changeComponentInstance("commander", commanderId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
