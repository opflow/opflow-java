package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowInterface;
import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowRpcListener;
import com.devebot.opflow.OpflowRpcResponse;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInterceptionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowInterfaceTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowInterfaceTest.class);
    private OpflowImplement serverlet;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        serverlet = new OpflowImplement();
    }
    
    @After
    public void afterEach() {
        if (serverlet != null) serverlet.close();
    }
    
    public interface If {
        String originalMethod(String s);
    }
    
    public interface Calculator {
        Integer add(Integer a);
        Integer add(Integer a, Integer b);
    }
    
    public static class CalculatorImpl implements Calculator {
        
        @Override
        public Integer add(Integer a) {
            return a + 1;
        }
        
        @Override
        public Integer add(Integer a, Integer b) {
            return a + b;
        }
    }
    
    public static class OpflowImplement {
        private static final Logger LOG = LoggerFactory.getLogger(OpflowImplement.class);
        private final OpflowRpcWorker rpcWorker;
        private final OpflowRpcListener listener;
        private final Set<String> routineIds = new HashSet<String>();
        private final Map<String, Method> methodRef = new HashMap<String, Method>();
        private final Map<String, Object> targetRef = new HashMap<String, Object>();
        
        public OpflowImplement() throws OpflowBootstrapException {
            this(null);
        }
        
        public OpflowImplement(OpflowRpcWorker worker) throws OpflowBootstrapException {
            rpcWorker = (worker != null) ? worker : OpflowLoader.createRpcWorker();
            listener = new OpflowRpcListener() {
                @Override
                public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                    String routineId = OpflowUtil.getRoutineId(message.getInfo());
                    if (LOG.isDebugEnabled()) LOG.debug(" - Request routineId: " + routineId);
                    String json = message.getBodyAsString();
                    if (LOG.isDebugEnabled()) LOG.debug(" - Request body: " + json);
                    Object[] args = OpflowUtil.jsonStringToArray(json, methodRef.get(routineId).getParameterTypes());
                    try {
                        Object result = methodRef.get(routineId).invoke(targetRef.get(routineId), args);
                        if (LOG.isDebugEnabled()) LOG.debug(" - Output: " + result);
                        response.emitCompleted(OpflowUtil.jsonObjectToString(result));
                    } catch (IllegalAccessException ex) {
                        LOG.error(null, ex);
                    } catch (IllegalArgumentException ex) {
                        LOG.error(null, ex);
                    } catch (InvocationTargetException ex) {
                        LOG.error(null, ex);
                    }
                    return null;
                }
            };
            rpcWorker.process(routineIds, listener);
        }
        
        public void instantiateType(Class type) {
            if (Modifier.isAbstract(type.getModifiers())) {
                throw new OpflowInterceptionException("Class should not be an abstract type");
            }
            try {
                List<Class> clazzes = new LinkedList<Class>();
                clazzes.add(type);
                Class[] interfaces = type.getInterfaces();
                clazzes.addAll(Arrays.asList(interfaces));

                Object target = type.newInstance();
                for(Class clz: clazzes) {
                    Method[] methods = clz.getDeclaredMethods();
                    for (Method method : methods) {
                        String routineId = method.toString();
                        if (LOG.isTraceEnabled()) LOG.trace(" - Attach method: " + routineId);
                        routineIds.add(routineId);
                        methodRef.put(routineId, method);
                        targetRef.put(routineId, target);
                    }
                }
            } catch (InstantiationException except) {
                throw new OpflowInterceptionException("Could not instantiate the class", except);
            } catch (IllegalAccessException except) {
                throw new OpflowInterceptionException("Constructor is not accessible", except);
            } catch (SecurityException except) {
                throw new OpflowInterceptionException("Class loaders is not the same or denies access", except);
            } catch (Exception except) {
                throw new OpflowInterceptionException(OpflowUtil.buildMap()
                        .put("errorType", except.getClass().getName())
                        .put("errorMessage", except.getMessage())
                        .put("message", "Unknown exception")
                        .toString(), except);
            }
        }
        
        public void close() {
            if (rpcWorker != null) rpcWorker.close();
        }
    }
    
    @Test
    public void testCreateRpcMaster() throws OpflowBootstrapException {
        Calculator calc = OpflowInterface.getInstance().registerType(Calculator.class);
        serverlet.instantiateType(CalculatorImpl.class);
        for(int k=0; k<1000; k++) {
            System.out.println("Result: " + calc.add(100, k));
        }
        System.out.println("Result: " + calc.add(500));
    }
}
