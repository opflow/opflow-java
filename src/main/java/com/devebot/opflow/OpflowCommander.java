package com.devebot.opflow;

import com.devebot.opflow.OpflowUtil.MapBuilder;
import com.devebot.opflow.annotation.OpflowSourceRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import io.undertow.server.HttpHandler;
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
        "restServer", "rpcWatcher"
    });

    public final static List<String> ALL_BEAN_NAMES = OpflowUtil.mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);

    public final static String PARAM_RESERVE_WORKER_ENABLED = "reserveWorkerEnabled";

    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommander.class);
    private final OpflowLogTracer logTracer;
    private final OpflowExporter exporter;

    private final String commanderId;

    private boolean reserveWorkerEnabled;
    private OpflowPubsubHandler configurer;
    private OpflowRpcMaster rpcMaster;
    private OpflowPubsubHandler publisher;
    private OpflowRpcChecker rpcChecker;
    private OpflowRpcWatcher rpcWatcher;
    private OpflowRestServer restServer;

    public OpflowCommander() throws OpflowBootstrapException {
        this(null);
    }

    public OpflowCommander(Map<String, Object> kwargs) throws OpflowBootstrapException {
        kwargs = OpflowUtil.ensureNotNull(kwargs);
        commanderId = OpflowUtil.getOptionField(kwargs, "commanderId", true);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", commanderId);

        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].new()")
                .stringify());

        if (kwargs.get(PARAM_RESERVE_WORKER_ENABLED) != null && kwargs.get(PARAM_RESERVE_WORKER_ENABLED) instanceof Boolean) {
            reserveWorkerEnabled = (Boolean) kwargs.get(PARAM_RESERVE_WORKER_ENABLED);
        } else {
            reserveWorkerEnabled = true;
        }

        Map<String, Object> configurerCfg = (Map<String, Object>)kwargs.get("configurer");
        Map<String, Object> rpcMasterCfg = (Map<String, Object>)kwargs.get("rpcMaster");
        Map<String, Object> publisherCfg = (Map<String, Object>)kwargs.get("publisher");
        Map<String, Object> rpcWatcherCfg = (Map<String, Object>)kwargs.get("rpcWatcher");
        Map<String, Object> restServerCfg = (Map<String, Object>)kwargs.get("restServer");

        HashSet<String> checkExchange = new HashSet<>();

        if (configurerCfg != null && !Boolean.FALSE.equals(configurerCfg.get("enabled"))) {
            if (OpflowUtil.isAMQPEntrypointNull(configurerCfg)) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(configurerCfg))) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters");
            }
        }

        if (rpcMasterCfg != null && !Boolean.FALSE.equals(rpcMasterCfg.get("enabled"))) {
            if (OpflowUtil.isAMQPEntrypointNull(rpcMasterCfg)) {
                throw new OpflowBootstrapException("Invalid RpcMaster connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(rpcMasterCfg))) {
                throw new OpflowBootstrapException("Duplicated RpcMaster connection parameters");
            }
        }

        if (publisherCfg != null && !Boolean.FALSE.equals(publisherCfg.get("enabled"))) {
            if (OpflowUtil.isAMQPEntrypointNull(publisherCfg)) {
                throw new OpflowBootstrapException("Invalid Publisher connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(publisherCfg))) {
                throw new OpflowBootstrapException("Duplicated Publisher connection parameters");
            }
        }

        try {
            if (configurerCfg != null && !Boolean.FALSE.equals(configurerCfg.get("enabled"))) {
                configurer = new OpflowPubsubHandler(configurerCfg);
            }
            if (rpcMasterCfg != null && !Boolean.FALSE.equals(rpcMasterCfg.get("enabled"))) {
                rpcMaster = new OpflowRpcMaster(rpcMasterCfg);
            }
            if (publisherCfg != null && !Boolean.FALSE.equals(publisherCfg.get("enabled"))) {
                publisher = new OpflowPubsubHandler(publisherCfg);
            }

            rpcChecker = new OpflowRpcCheckerMaster(rpcMaster);

            rpcWatcher = new OpflowRpcWatcher(rpcChecker, OpflowUtil.buildMap(rpcWatcherCfg)
                    .put("instanceId", commanderId)
                    .toMap());

            rpcWatcher.start();

            OpflowInfoCollector infoCollector = new OpflowInfoCollectorMaster(commanderId, rpcMaster, handlers);

            OpflowTaskSubmitter taskSubmitter = new OpflowTaskSubmitterMaster(commanderId, rpcMaster);
            
            restServer = new OpflowRestServer(infoCollector, taskSubmitter, rpcChecker, OpflowUtil.buildMap(restServerCfg)
                    .put("instanceId", commanderId)
                    .toMap());
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }

        exporter = OpflowExporter.getInstance();

        exporter.changeComponentInstance("commander", commanderId, OpflowExporter.GaugeAction.INC);

        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].new() end!")
                .stringify());
    }

    public boolean isReserveWorkerEnabled() {
        return this.reserveWorkerEnabled;
    }

    public void setReserveWorkerEnabled(boolean enabled) {
        this.reserveWorkerEnabled = enabled;
    }

    public Map<String, HttpHandler> getInfoHttpHandlers() {
        if (restServer != null) {
            return restServer.getHttpHandlers();
        }
        return null;
    }

    public OpflowRpcChecker.Info ping() {
        if (restServer != null) {
            return restServer.ping();
        }
        return null;
    }

    public final void serve() {
        if (restServer != null) {
            restServer.serve();
        }
    }

    public final void serve(Map<String, HttpHandler> httpHandlers) {
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
        if (publisher != null) publisher.close();
        if (rpcMaster != null) rpcMaster.close();
        if (configurer != null) configurer.close();

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
            String body = OpflowJsontool.toString(new Object[] { ping });
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
                Map<String, Object> errorMap = OpflowJsontool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            Pong pong = OpflowJsontool.toObject(rpcResult.getValueAsString(), Pong.class);
            pong.getParameters().put("requestId", requestId);
            pong.getParameters().put("startTime", OpflowUtil.toISO8601UTC(startTime));
            pong.getParameters().put("endTime", OpflowUtil.toISO8601UTC(endTime));
            pong.getParameters().put("duration", endTime.getTime() - startTime.getTime());
            return pong;
        }
    }

    private static class OpflowTaskSubmitterMaster implements OpflowTaskSubmitter {

        private final String instanceId;
        private final OpflowLogTracer logTracer;
        private final OpflowRpcMaster rpcMaster;

        public OpflowTaskSubmitterMaster(String instanceId, OpflowRpcMaster rpcMaster) {
            this.instanceId = instanceId;
            this.rpcMaster = rpcMaster;
            this.logTracer = OpflowLogTracer.ROOT.branch("taskSubmitterId", instanceId);
        }
        
        @Override
        public void reset() {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].reset() is invoked")
                    .stringify());
            rpcMaster.close();
        }

        @Override
        public void pause() {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("OpflowTaskSubmitter[${taskSubmitterId}].pause(true) is invoked")
                    .stringify());
            rpcMaster.pause();
        }
    }
    
    private static class OpflowInfoCollectorMaster implements OpflowInfoCollector {

        private final String instanceId;
        private final OpflowRpcMaster rpcMaster;
        private final Map<String, RpcInvocationHandler> handlers;

        OpflowInfoCollectorMaster(String instanceId, OpflowRpcMaster rpcMaster, Map<String, RpcInvocationHandler> mappings) {
            this.instanceId = instanceId;
            this.rpcMaster = rpcMaster;
            this.handlers = mappings;
        }

        @Override
        public Map<String, Object> collect() {
            return collect(null);
        }

        @Override
        public Map<String, Object> collect(Scope scope) {
            final Scope label = (scope == null) ? Scope.BASIC : scope;
            return OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
                @Override
                public void transform(Map<String, Object> opts) {
                    OpflowEngine engine = rpcMaster.getEngine();
                    opts.put("instanceId", instanceId);

                    // rpcMaster information
                    MapBuilder mb1 = OpflowUtil.buildOrderedMap()
                            .put("instanceId", rpcMaster.getInstanceId())
                            .put("applicationId", engine.getApplicationId())
                            .put("exchangeName", engine.getExchangeName());

                    if (label == Scope.FULL) {
                        mb1.put("exchangeDurable", engine.getExchangeDurable());
                    }

                    mb1.put("routingKey", engine.getRoutingKey());

                    if (label == Scope.FULL) {
                        mb1.put("otherKeys", engine.getOtherKeys());
                    }

                    mb1.put("callbackQueue", rpcMaster.getCallbackName());

                    if (label == Scope.FULL) {
                        mb1.put("callbackDurable", rpcMaster.getCallbackDurable())
                                .put("callbackExclusive", rpcMaster.getCallbackExclusive())
                                .put("callbackAutoDelete", rpcMaster.getCallbackAutoDelete());
                    }
                    opts.put("rpcMaster", mb1.toMap());

                    // RPC mappings
                    if (label == Scope.FULL) {
                        List<Map<String, Object>> mappingInfos = new ArrayList<>();
                        for(Map.Entry<String, RpcInvocationHandler> entry : handlers.entrySet()) {
                            mappingInfos.add(OpflowUtil.buildOrderedMap()
                                    .put("class", entry.getKey())
                                    .put("methods", entry.getValue().getMethodNames())
                                    .put("isReserveWorkerReady", entry.getValue().hasReserveWorker())
                                    .toMap());
                        }
                        opts.put("mappings", mappingInfos);
                    }

                    // request information
                    opts.put("request", OpflowUtil.buildOrderedMap()
                            .put("expiration", rpcMaster.getExpiration())
                            .toMap());
                }
            }).toMap();
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

        public Set<String> getMethodNames() {
            return methodIsAsync.keySet();
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final String pingSignature = OpflowRpcCheckerMaster.getSendMethodName();
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
            String body = OpflowJsontool.toString(args);

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
            if (rpcWatcher.isCongested() && !pingSignature.equals(routineId)) {
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
                Map<String, Object> errorMap = OpflowJsontool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            if (logRequest.ready(LOG, "trace")) LOG.trace(logRequest
                    .put("returnType", method.getReturnType().getName())
                    .put("returnValue", rpcResult.getValueAsString())
                    .text("Request[${requestId}] - RpcInvocationHandler.invoke() return the output")
                    .stringify());

            if (method.getReturnType() == void.class) return null;

            return OpflowJsontool.toObject(rpcResult.getValueAsString(), method.getReturnType());
        }
    }

    private static Throwable rebuildInvokerException(Map<String, Object> errorMap) {
        Object exceptionName = errorMap.get("exceptionClass");
        Object exceptionPayload = errorMap.get("exceptionPayload");
        if (exceptionName != null && exceptionPayload != null) {
            try {
                Class exceptionClass = Class.forName(exceptionName.toString());
                return (Throwable) OpflowJsontool.toObject(exceptionPayload.toString(), exceptionClass);
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
        exporter.changeComponentInstance("commander", commanderId, OpflowExporter.GaugeAction.DEC);
    }
}
