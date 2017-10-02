package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 *
 * @author drupalex
 */
public class OpflowCommanderTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowCommanderTest.class);
    private OpflowRpcWorker rpcWorker;
    private OpflowServerlet.Instantiator instantiator;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        rpcWorker = OpflowBuilder.createRpcWorker();
        instantiator = new OpflowServerlet.Instantiator(rpcWorker, OpflowUtil.buildMap()
                .put("autorun", true).toMap());
    }
    
    @After
    public void afterEach() {
        if (rpcWorker != null) rpcWorker.close();
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
            if (count > 1) throw new CalculatorException("this is a demo");
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
    public void testMassCallingMethods() throws OpflowBootstrapException {
        Calculator calc = OpflowBuilder.createCommander("commander.properties").registerType(Calculator.class);
        instantiator.instantiateType(CalculatorImpl.class);
        for(int k=0; k<100; k++) {
            // System.out.println("Result: " + calc.add(100, k));
            assertThat(calc.add(100, k), equalTo(100+k));
        }
        calc.printInfo();
    }
    
    @Test
    public void testThrowException() throws CalculatorException, OpflowBootstrapException {
        Calculator calc = OpflowBuilder.createCommander("commander.properties").registerType(Calculator.class);
        instantiator.instantiateType(CalculatorImpl.class);
        thrown.expect(CalculatorException.class);
        thrown.expectMessage(CoreMatchers.is("this is a demo"));
        try {
            assertThat(calc.tick(), equalTo(1));
            assertThat(calc.tick(), not(equalTo(2)));
        } catch (CalculatorException exception) {
            //System.out.println("Exception: " + exception.getClass().getName() + "/" + exception.getMessage());
            //exception.printStackTrace();
            throw exception;
        }
    }
}
