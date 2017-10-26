package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.annotation.OpflowSourceRoutine;

/**
 *
 * @author drupalex
 */
public class OpflowCommander {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommander.class);
    private final OpflowLogTracer logTracer;
    
    private OpflowPubsubHandler configurer;
    private OpflowRpcMaster rpcMaster;
    private OpflowPubsubHandler publisher;
    
    public OpflowCommander() throws OpflowBootstrapException {
        this(null);
    }
    
    public OpflowCommander(Map<String, Object> kwargs) throws OpflowBootstrapException {
        kwargs = OpflowUtil.ensureNotNull(kwargs);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", OpflowUtil.getOptionField(kwargs, "commanderId", true));
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Commander.new()")
                .toString());

        Map<String, Object> configurerCfg = (Map<String, Object>)kwargs.get("configurer");
        Map<String, Object> rpcMasterCfg = (Map<String, Object>)kwargs.get("rpcMaster");
        Map<String, Object> publisherCfg = (Map<String, Object>)kwargs.get("publisher");
        
        HashSet<String> checkExchange = new HashSet<String>();
        
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
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Commander.new() end!")
                .toString());
    }
    
    public final void close() {
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Commander stop()")
                .toString());
        
        if (configurer != null) configurer.close();
        if (rpcMaster != null) rpcMaster.close();
        if (publisher != null) publisher.close();
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Commander stop() has done!")
                .toString());
    }
    
    private class RpcInvocationHandler implements InvocationHandler {
        private final Class clazz;
        private final Map<String, String> aliasOfMethod = new HashMap<String, String>();
        private final OpflowRpcMaster rpcMaster;
        
        public RpcInvocationHandler(OpflowRpcMaster rpcMaster, Class clazz) {
            this.clazz = clazz;
            for (Method method : this.clazz.getDeclaredMethods()) {
                String methodId = OpflowUtil.getMethodSignature(method);
                OpflowSourceRoutine routine = extractMethodInfo(method);
                if (routine != null && routine.alias() != null && routine.alias().length() > 0) {
                    String alias = routine.alias();
                    if (aliasOfMethod.containsValue(alias)) {
                        throw new OpflowInterceptionException("Alias[" + alias + "]/routineId[" + methodId + "] is duplicated");
                    }
                    aliasOfMethod.put(methodId, alias);
                    if (LOG.isTraceEnabled()) LOG.trace(logTracer
                            .put("alias", alias)
                            .put("routineId", methodId)
                            .put("message", "link alias to routineId")
                            .stringify(true));
                }
            }
            this.rpcMaster = rpcMaster;
        }
        
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodId = OpflowUtil.getMethodSignature(method);
            String routineId = aliasOfMethod.getOrDefault(methodId, methodId);
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                    .put("methodId", methodId)
                    .put("routineId", routineId)
                    .put("message", "RpcInvocationHandler.invoke()")
                    .toString());
            
            if (args == null) args = new Object[0];
            String body = OpflowJsontool.toString(args);
            
            if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                    .put("args", args)
                    .put("body", body)
                    .put("message", "RpcInvocationHandler.invoke() - request")
                    .toString());
            
            OpflowRpcRequest rpcSession = rpcMaster.request(routineId, body, OpflowUtil.buildMap()
                    .put("progressEnabled", false).toMap());
            OpflowRpcResult rpcResult = rpcSession.extractResult(false);
            
            if (rpcResult.isTimeout()) {
                throw new OpflowRequestTimeoutException();
            }
            
            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsontool.toObjectMap(rpcResult.getErrorAsString());
                throw rebuildInvokerException(errorMap);
            }
            
            if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                    .put("returnType", method.getReturnType().getName())
                    .put("returnValue", rpcResult.getValueAsString())
                    .put("message", "RpcInvocationHandler.invoke() - response")
                    .toString());
            
            if (method.getReturnType() == void.class) return null;
            
            return OpflowJsontool.toObject(rpcResult.getValueAsString(), method.getReturnType());
        }
        
        private Throwable rebuildInvokerException(Map<String, Object> errorMap) {
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
        
        private Throwable rebuildFailureException(Map<String, Object> errorMap) {
            if (errorMap.get("message") != null) {
                return new OpflowRequestFailureException(errorMap.get("message").toString());
            }
            return new OpflowRequestFailureException();
        }
    }
    
    private final Map<String, RpcInvocationHandler> handlers = new HashMap<String, RpcInvocationHandler>();
    
    private RpcInvocationHandler getInvocationHandler(Class clazz) {
        validateType(clazz);
        String clazzName = clazz.getName();
        if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                .put("className", clazzName)
                .put("message", "getInvocationHandler() get InvocationHandler by type")
                .toString());
        if (!handlers.containsKey(clazzName)) {
            if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                    .put("className", clazzName)
                    .put("message", "getInvocationHandler() InvocationHandler not found, create new one")
                    .toString());
            handlers.put(clazzName, new RpcInvocationHandler(rpcMaster, clazz));
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
            if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                    .put("typeString", type.toGenericString())
                    .put("message", "generic types are unsupported")
                    .toString());
        }
        Method[] methods = type.getDeclaredMethods();
        for(Method method:methods) {
            if (OpflowUtil.isGenericDeclaration(method.toGenericString())) {
                ok = false;
                if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                        .put("methodString", method.toGenericString())
                        .put("message", "generic methods are unsupported")
                        .toString());
            }
        }
        if (!ok) {
            throw new OpflowInterceptionException("Generic type/method is unsupported");
        }
        return ok;
    }
    
    public <T> T registerType(Class<T> type) {
        try {
            if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                    .put("className", type.getName())
                    .put("classLoaderName", type.getClassLoader().getClass().getName())
                    .put("message", "registerType() calls newProxyInstance()")
                    .toString());
            T t = (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type));
            if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                    .put("className", type.getName())
                    .put("message", "newProxyInstance() has completed")
                    .toString());
            return t;
        } catch (IllegalArgumentException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "newProxyInstance() has failed")
                    .toString());
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
}
