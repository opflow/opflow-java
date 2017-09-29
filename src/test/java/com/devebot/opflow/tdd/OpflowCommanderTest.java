package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.OpflowUtil;
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
public class OpflowCommanderTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowCommanderTest.class);
    private OpflowRpcWorker rpcWorker;
    private OpflowServerlet.Instantiator serverlet;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        rpcWorker = OpflowBuilder.createRpcWorker();
        serverlet = new OpflowServerlet.Instantiator(rpcWorker, OpflowUtil.buildMap()
                .put("autorun", true).toMap());
    }
    
    @After
    public void afterEach() {
        if (rpcWorker != null) rpcWorker.close();
    }
    
    public interface If {
        String originalMethod(String s);
    }
    
    public interface Calculator {
        Integer tick() throws CalculatorException;
        Integer add(Integer a);
        Integer add(Integer a, Integer b);
        void printInfo();
    }
    
    public static class CalculatorImpl implements Calculator {
        int count = 0;
        
        @Override
        public Integer tick() throws CalculatorException {
            ++count;
            if (count > 1) throw new CalculatorException("demo");
            return count;
        }
        
        @Override
        public Integer add(Integer a) {
            return a + 1;
        }
        
        @Override
        public Integer add(Integer a, Integer b) {
            return a + b;
        }
        
        @Override
        public void printInfo() {
            System.out.println("Hello world");
        }
    }
    
    public static class CalculatorException extends Exception {

        public CalculatorException() {
        }

        public CalculatorException(String message) {
            super(message);
        }

        public CalculatorException(String message, Throwable cause) {
            super(message, cause);
        }

        public CalculatorException(Throwable cause) {
            super(cause);
        }
    }
    
    @Test
    public void testCreateRpcMaster() throws OpflowBootstrapException {
        Calculator calc = OpflowBuilder.createCommander("commander.properties").registerType(Calculator.class);
        serverlet.instantiateType(CalculatorImpl.class);
        for(int k=0; k<100; k++) {
            System.out.println("Result: " + calc.add(100, k));
        }
        try {
            System.out.println("Result: " + calc.tick());
            System.out.println("Result: " + calc.tick());
            System.out.println("Result: " + calc.tick());
        } catch (CalculatorException exception) {
            System.out.println("Exception: " + exception.getClass().getName() + "/" + exception.getMessage());
            exception.printStackTrace();
        }
        
        calc.printInfo();
    }
}
