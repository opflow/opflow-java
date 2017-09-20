package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowFactory;
import com.devebot.opflow.exception.OpflowBootstrapException;
import org.junit.Test;

/**
 *
 * @author drupalex
 */
public class OpflowFactoryTest {
    
    public interface If {
        String originalMethod(String s);
    }
    
    public interface Calculator {
        Integer add(Integer a);
        Integer add(Integer a, Integer b);
    }
    
//    @Test
    public void testCreateRpcMaster() throws OpflowBootstrapException {
        Calculator calc = OpflowFactory.getInstance().buildRpcMaster(Calculator.class);
        System.out.println("Result: " + calc.add(100, 200));
    }
    
    @Test
    public void testBuildRpcWorker() throws OpflowBootstrapException {
        OpflowFactory.getInstance().buildRpcWorker(Calculator.class);
    }
}
