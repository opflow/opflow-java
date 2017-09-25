package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptorException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowInterface {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowInterface.class);
    private final static OpflowLogTracer logTracer = OpflowLogTracer.ROOT.copy();
    private final OpflowRpcMaster rpcMaster;
    
    private OpflowInterface() throws OpflowBootstrapException {
        rpcMaster = OpflowLoader.createRpcMaster();
    }
    
    private static class MasterInvocationHandler implements InvocationHandler {
        private final Class clazz;
        private final OpflowRpcMaster rpcMaster;
        
        public MasterInvocationHandler(OpflowRpcMaster rpcMaster, Class clazz) {
            this.clazz = clazz;
            this.rpcMaster = rpcMaster;
        }
        
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws InvocationTargetException {
            String routineId = clazz.getName() + "/" + method.toString();
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                    .put("routineId", routineId)
                    .put("message", "MasterInvocationHandler.invoke()")
                    .toString());
            
            String body = OpflowUtil.jsonObjectToString(args);
            
            OpflowRpcRequest rpcSession = rpcMaster.request(routineId, body);
            OpflowRpcResult rpcResult = rpcSession.extractResult(false);
            
            return OpflowUtil.jsonStringToObject(rpcResult.getValueAsString(), method.getReturnType());
        }
    }
    
    private final Map<String, MasterInvocationHandler> handlers = new HashMap<String, MasterInvocationHandler>();
    
    private MasterInvocationHandler getInvocationHandler(Class clazz) {
        String clazzName = clazz.getName();
        if (!handlers.containsKey(clazzName)) {
            handlers.put(clazzName, new MasterInvocationHandler(rpcMaster, clazz));
        }
        return handlers.get(clazzName);
    }
    
    private void removeInvocationHandler(Class clazz) {
        if (clazz == null) return;
        String clazzName = clazz.getName();
        handlers.remove(clazzName);
    }
    
    public <T> T registerInterface(Class<T> type) {
        try {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type));
        } catch (IllegalArgumentException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "newProxyInstance() has failed")
                    .toString());
            throw new OpflowInterceptorException(exception);
        }
    }
    
    public <T> void unregisterInterface(Class<T> type) {
        removeInvocationHandler(type);
    }
    
    private static OpflowInterface INSTANCE;

    public static OpflowInterface getInstance() throws OpflowBootstrapException {
        if (INSTANCE == null) INSTANCE =  new OpflowInterface();
        return INSTANCE;
    }
}
