package com.devebot.opflow;

import com.devebot.opflow.annotation.OpflowSourceRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import com.devebot.opflow.supports.OpflowRpcChecker;
import com.devebot.opflow.supports.OpflowRpcSwitcher;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowCommander implements AutoCloseable {
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
    private OpflowRpcSwitcher rpcSwitcher;
    
    public OpflowCommander() throws OpflowBootstrapException {
        this(null);
    }
    
    public OpflowCommander(Map<String, Object> kwargs) throws OpflowBootstrapException {
        kwargs = OpflowUtil.ensureNotNull(kwargs);
        commanderId = OpflowUtil.getOptionField(kwargs, "commanderId", true);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", commanderId);
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
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
        
        HashSet<String> checkExchange = new HashSet<>();
        
        if (configurerCfg != null && !Boolean.FALSE.equals(configurerCfg.get("enabled"))) {
            if (configurerCfg.get("exchangeName") == null || configurerCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid Configurer connection parameters");
            } 
            if (!checkExchange.add(configurerCfg.get("exchangeName").toString() + configurerCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated Configurer connection parameters");
            }
        }
        
        if (rpcMasterCfg != null && !Boolean.FALSE.equals(rpcMasterCfg.get("enabled"))) {
            if (rpcMasterCfg.get("exchangeName") == null || rpcMasterCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid RpcMaster connection parameters");
            }
            if (!checkExchange.add(rpcMasterCfg.get("exchangeName").toString() + rpcMasterCfg.get("routingKey").toString())) {
                throw new OpflowBootstrapException("Duplicated RpcMaster connection parameters");
            }
        }
        
        if (publisherCfg != null && !Boolean.FALSE.equals(publisherCfg.get("enabled"))) {
            if (publisherCfg.get("exchangeName") == null || publisherCfg.get("routingKey") == null) {
                throw new OpflowBootstrapException("Invalid Publisher connection parameters");
            }
            if (!checkExchange.add(publisherCfg.get("exchangeName").toString() + publisherCfg.get("routingKey").toString())) {
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
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
        
        rpcChecker = new OpflowRpcCheckerMaster(rpcMaster);
        
        rpcSwitcher = new OpflowRpcSwitcher(rpcChecker);
        rpcSwitcher.start();
        
        exporter = OpflowExporter.getInstance();
        
        exporter.changeComponentInstance("commander", commanderId, OpflowExporter.GaugeAction.INC);
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].new() end!")
                .stringify());
    }
    
    public boolean isReserveWorkerEnabled() {
        return this.reserveWorkerEnabled;
    }
    
    public void setReserveWorkerEnabled(boolean enabled) {
        this.reserveWorkerEnabled = enabled;
    }
    
    public OpflowRpcChecker.Info ping() {
        Map<String, Object> me = OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                OpflowEngine engine = rpcMaster.getEngine();
                opts.put("instanceId", commanderId);
                opts.put("rpcMaster", OpflowUtil.buildOrderedMap()
                        .put("instanceId", rpcMaster.getInstanceId())
                        .put("exchangeName", engine.getExchangeName())
                        .put("exchangeDurable", engine.getExchangeDurable())
                        .put("routingKey", engine.getRoutingKey())
                        .put("otherKeys", engine.getOtherKeys())
                        .put("applicationId", engine.getApplicationId())
                        .put("callbackQueue", rpcMaster.getCallbackName())
                        .put("callbackDurable", rpcMaster.getCallbackDurable())
                        .put("callbackExclusive", rpcMaster.getCallbackExclusive())
                        .put("callbackAutoDelete", rpcMaster.getCallbackAutoDelete())
                        .toMap());
                opts.put("request", OpflowUtil.buildOrderedMap()
                        .put("expiration", rpcMaster.getExpiration())
                        .toMap());
            }
        }).toMap();
        try {
            return new OpflowRpcChecker.Info(me, this.rpcChecker.send(new OpflowRpcChecker.Ping()));
        } catch (Throwable exception) {
            return new OpflowRpcChecker.Info(me, exception);
        }
    }
    
    @Override
    public final void close() {
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].close()")
                .stringify());
        
        if (configurer != null) configurer.close();
        if (rpcMaster != null) rpcMaster.close();
        if (publisher != null) publisher.close();
        if (rpcSwitcher != null) rpcSwitcher.close();
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Commander[${commanderId}].close() has done!")
                .stringify());
    }
    
    private static class OpflowRpcCheckerMaster extends OpflowRpcChecker {
        
        private final OpflowRpcMaster rpcMaster;
        
        OpflowRpcCheckerMaster(OpflowRpcMaster rpcMaster) throws OpflowBootstrapException {
            this.rpcMaster = rpcMaster;
        }
        
        @Override
        public OpflowRpcChecker.Pong send(OpflowRpcChecker.Ping ping) throws Throwable {
            String body = OpflowJsontool.toString(new Object[] { ping });
            OpflowRpcRequest rpcRequest = rpcMaster.request(getSendMethodName(), body, OpflowUtil.buildMap()
                .put("progressEnabled", false).toMap());
            OpflowRpcResult rpcResult = rpcRequest.extractResult(false);

            if (rpcResult.isTimeout()) {
                throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
            }

            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsontool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }

            return OpflowJsontool.toObject(rpcResult.getValueAsString(), OpflowRpcChecker.Pong.class);
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
                    if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
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
        
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            final String pingSignature = OpflowRpcCheckerMaster.getSendMethodName();
            String methodId = OpflowUtil.getMethodSignature(method);
            String routineId = aliasOfMethod.getOrDefault(methodId, methodId);
            Boolean isAsync = methodIsAsync.getOrDefault(methodId, false);
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .put("methodId", methodId)
                    .put("routineId", routineId)
                    .text("RpcInvocationHandler.invoke()")
                    .stringify());
            
            if (args == null) args = new Object[0];
            String body = OpflowJsontool.toString(args);
            
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                    .put("args", args)
                    .put("body", body)
                    .text("RpcInvocationHandler.invoke() - request")
                    .stringify());
            
            if (this.publisher != null && isAsync && method.getReturnType() == void.class) {
                this.publisher.publish(body);
                return null;
            }
            
            // rpc switching
            if (rpcSwitcher.isCongested() && !pingSignature.equals(routineId)) {
                if (this.hasReserveWorker()) {
                    return method.invoke(this.reserveWorker, args);
                }
            }
            
            OpflowRpcRequest rpcSession = rpcMaster.request(routineId, body, OpflowUtil.buildMap()
                    .put("progressEnabled", false).toMap());
            OpflowRpcResult rpcResult = rpcSession.extractResult(false);
            
            if (rpcResult.isTimeout()) {
                rpcSwitcher.setCongested(true);
                if (this.hasReserveWorker()) {
                    return method.invoke(this.reserveWorker, args);
                }
                throw new OpflowRequestTimeoutException();
            }
            
            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsontool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }
            
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                    .put("returnType", method.getReturnType().getName())
                    .put("returnValue", rpcResult.getValueAsString())
                    .text("RpcInvocationHandler.invoke() - response")
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
    
    private final Map<String, RpcInvocationHandler> handlers = new HashMap<>();
    
    private RpcInvocationHandler getInvocationHandler(Class clazz, Object bean) {
        validateType(clazz);
        String clazzName = clazz.getName();
        if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                .put("className", clazzName)
                .text("getInvocationHandler() get InvocationHandler by type")
                .stringify());
        if (!handlers.containsKey(clazzName)) {
            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
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
            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                    .put("typeString", type.toGenericString())
                    .text("generic types are unsupported")
                    .stringify());
        }
        Method[] methods = type.getDeclaredMethods();
        for(Method method:methods) {
            if (OpflowUtil.isGenericDeclaration(method.toGenericString())) {
                ok = false;
                if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
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
            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                    .put("className", type.getName())
                    .put("classLoaderName", type.getClassLoader().getClass().getName())
                    .text("registerType() calls newProxyInstance()")
                    .stringify());
            T t = (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type, bean));
            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                    .put("className", type.getName())
                    .text("newProxyInstance() has completed")
                    .stringify());
            return t;
        } catch (IllegalArgumentException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
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
