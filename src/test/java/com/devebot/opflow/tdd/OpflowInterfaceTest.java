package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowInterface;
import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.exception.OpflowBootstrapException;
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
    private OpflowServerlet.Instantiator serverlet;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        rpcWorker = OpflowLoader.createRpcWorker();
        serverlet = new OpflowServerlet.Instantiator(rpcWorker, true);
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
    public void testCreateRpcMaster() throws OpflowBootstrapException {
        Calculator calc = OpflowInterface.getInstance().registerType(Calculator.class);
        serverlet.instantiateType(CalculatorImpl.class);
        for(int k=0; k<1000; k++) {
            System.out.println("Result: " + calc.add(100, k));
        }
        System.out.println("Result: " + calc.add(500));
    }
}
