package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowRpcMaster;
import com.devebot.opflow.OpflowRpcResult;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.annotation.OpflowRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author drupalex
 */
public class OpflowInstantiatorTest {
    private OpflowRpcMaster rpcMaster;
    private OpflowRpcWorker rpcWorker;
    private OpflowServerlet.Instantiator instantiator;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        rpcMaster = OpflowBuilder.createRpcMaster();
        rpcWorker = OpflowBuilder.createRpcWorker();
        instantiator = new OpflowServerlet.Instantiator(rpcWorker, OpflowUtil.buildMap()
                .put("autorun", true).toMap());
    }
    
    @After
    public void afterEach() {
        if (rpcMaster != null) rpcMaster.close();
        if (rpcWorker != null) rpcWorker.close();
    }
    
    @Test
    public void testRpcWorkerMethodAlias() throws OpflowBootstrapException, NoSuchMethodException {
        Calculator calc = OpflowBuilder.createCommander("commander.properties").registerType(Calculator.class);
        instantiator.instantiateType(CalculatorImpl.class);
        Assert.assertEquals(21, calc.add(20).intValue());
        OpflowRpcResult result = rpcMaster.request("increase", OpflowJsontool.toString(new Object[] {10})).extractResult();
        Assert.assertEquals(11, Integer.parseInt(result.getValueAsString()));
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
        @OpflowRoutine(alias={"increase"})
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

}
