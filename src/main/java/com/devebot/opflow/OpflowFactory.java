package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowRpcFactoryBeanException;
import java.io.IOException;
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
public class OpflowFactory {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowFactory.class);
    private final static OpflowLogTracer logTracer = OpflowLogTracer.ROOT.copy();
    private final OpflowRpcMaster rpcMaster;
    private final OpflowRpcWorker rpcWorker;
    
    private OpflowFactory() throws OpflowBootstrapException {
        rpcMaster = OpflowLoader.createRpcMaster();
        rpcWorker = OpflowLoader.createRpcWorker();
    }
    
    private static class MasterInvocationHandler implements InvocationHandler {
        private final Class clazz;
        private final OpflowRpcMaster rpcMaster;
        
        public MasterInvocationHandler(Class clazz, OpflowRpcMaster rpcMaster) {
            this.clazz = clazz;
            this.rpcMaster = rpcMaster;
        }
        
        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws IllegalAccessException, IllegalArgumentException,
                InvocationTargetException, NoSuchMethodException {
            String methodName = clazz.getName() + "." + method.getName();
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                    .put("methodName", methodName)
                    .put("message", "MasterInvocationHandler.invoke()")
                    .toString());
            
            String body = OpflowUtil.jsonObjectToString(args);
            
            OpflowRpcRequest rpcSession = rpcMaster.request(methodName, body);
            OpflowRpcResult rpcResult = rpcSession.extractResult(false);
            
            return OpflowUtil.jsonStringToObject(rpcResult.getValueAsString(), method.getReturnType());
        }
    }
    
    private final Map<String, MasterInvocationHandler> handlers = new HashMap<String, MasterInvocationHandler>();
    
    private MasterInvocationHandler getInvocationHandler(Class clazz) {
        String clazzName = clazz.getName();
        if (!handlers.containsKey(clazzName)) {
            handlers.put(clazzName, new MasterInvocationHandler(clazz, rpcMaster));
        }
        return handlers.get(clazzName);
    }
    
    public <T> T buildRpcMaster(Class<T> type) {
        try {
            return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type));
        } catch (IllegalArgumentException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "newProxyInstance() has failed")
                    .toString());
            throw new OpflowRpcFactoryBeanException(exception);
        }
    }
    
    private final Map<String, OpflowRpcListener> listeners = new HashMap<String, OpflowRpcListener>();
    
    private OpflowRpcListener getRpcListener(final Class clazz) {
        String clazzName = clazz.getName();
        if (!listeners.containsKey(clazzName)) {
            listeners.put(clazzName, new OpflowRpcListener() {
                @Override
                public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                    
                    return null;
                }
            });
        }
        return listeners.get(clazzName);
    }
    
    public <T> void buildRpcWorker(Class<T> type) {
        Method[] methods = type.getDeclaredMethods();
        for(int i=0; i<methods.length; i++) {
//            System.out.println("Method: " + methods[i].getName() + "/" + methods[i].getParameterCount());
            System.out.println("Method: " + methods[i].toString());
            System.out.println("Generic String: " + methods[i].toGenericString());
        }
//        type.getDeclaredMethods()
//        rpcWorker.process(, listener)
    }
    
    private static OpflowFactory INSTANCE;

    public static OpflowFactory getInstance() throws OpflowBootstrapException {
        if (INSTANCE == null) INSTANCE =  new OpflowFactory();
        return INSTANCE;
    }
}
