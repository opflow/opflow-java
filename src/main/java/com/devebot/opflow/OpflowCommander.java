package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import com.devebot.opflow.exception.OpflowRequestFailedException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
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
        private final OpflowRpcMaster rpcMaster;
        
        public RpcInvocationHandler(OpflowRpcMaster rpcMaster, Class clazz) {
            this.clazz = clazz;
            this.rpcMaster = rpcMaster;
        }
        
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws InvocationTargetException {
            String routineId = method.toString();
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                    .put("routineId", routineId)
                    .put("message", "RpcInvocationHandler.invoke()")
                    .toString());
            
            if (args == null) args = new Object[0];
            String body = OpflowUtil.jsonObjectToString(args);
            
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
                throw new OpflowRequestFailedException(rpcResult.getErrorAsString());
            }
            
            if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                    .put("returnType", method.getReturnType().getName())
                    .put("returnValue", rpcResult.getValueAsString())
                    .put("message", "RpcInvocationHandler.invoke() - response")
                    .toString());
            
            if (method.getReturnType() == void.class) return null;
            
            return OpflowUtil.jsonStringToObject(rpcResult.getValueAsString(), method.getReturnType());
        }
    }
    
    private final Map<String, RpcInvocationHandler> handlers = new HashMap<String, RpcInvocationHandler>();
    
    private RpcInvocationHandler getInvocationHandler(Class clazz) {
        String clazzName = clazz.getName();
        if (!handlers.containsKey(clazzName)) {
            handlers.put(clazzName, new RpcInvocationHandler(rpcMaster, clazz));
        }
        return handlers.get(clazzName);
    }
    
    private void removeInvocationHandler(Class clazz) {
        if (clazz == null) return;
        String clazzName = clazz.getName();
        handlers.remove(clazzName);
    }
    
    public <T> T registerType(Class<T> type) {
        try {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type));
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
}
