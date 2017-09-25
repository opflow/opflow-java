package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowInterface;
import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowRpcListener;
import com.devebot.opflow.OpflowRpcResponse;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
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
    private OpflowRpcWorker rpcWorker;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        rpcWorker = OpflowLoader.createRpcWorker();
    }
    
    @After
    public void afterEach() {
        if (rpcWorker != null) rpcWorker.close();
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
    
    @Test
    public void testCreateRpcMaster() throws OpflowBootstrapException, InstantiationException, IllegalAccessException {
        Calculator calc = OpflowInterface.getInstance().registerInterface(Calculator.class);
        
        Method[] methods = Calculator.class.getMethods();
        String[] routineIds = new String[methods.length];
        final Map<String, Method> methodRef = new HashMap<String, Method>();
        for(int i=0; i<methods.length; i++) {
            routineIds[i] = Calculator.class.getName() + "/" + methods[i].toString();
            methodRef.put(routineIds[i], methods[i]);
        }
        System.out.println("RoutineIDS: " + OpflowUtil.jsonObjectToString(routineIds));
        
        final Calculator calcImpl = CalculatorImpl.class.newInstance();
        
        rpcWorker.process(routineIds, new OpflowRpcListener() {
            @Override
            public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                String routineId = OpflowUtil.getRoutineId(message.getInfo());
                String json = message.getBodyAsString();
                System.out.println("Request Body: " + json);
                Object[] args = OpflowUtil.jsonStringToArray(json, methodRef.get(routineId).getParameterTypes());
                
                try {
                    Object result = methodRef.get(routineId).invoke(calcImpl, args);
                    System.out.println("Output: " + result);
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
        });
        System.out.println("Result: " + calc.add(100, 200));
    }
}
