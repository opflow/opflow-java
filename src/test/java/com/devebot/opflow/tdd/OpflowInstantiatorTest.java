package com.devebot.opflow.tdd;

import com.devebot.opflow.lab.SimpleCalculatorImpl;
import com.devebot.opflow.lab.SimpleCalculator;
import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowLogTracer;
import com.devebot.opflow.OpflowLogTracer.StringifyInterceptor;
import com.devebot.opflow.OpflowRpcMaster;
import com.devebot.opflow.OpflowRpcResult;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.lab.MirrorCalculator;
import com.devebot.opflow.lab.MirrorCalculatorImpl;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author drupalex
 */
public class OpflowInstantiatorTest {
    private OpflowRpcMaster rpcMaster;
    private OpflowRpcWorker rpcWorker;
    private OpflowServerlet.Instantiator instantiator;
    
    private static final Map<String, Integer> COUNTER = new HashMap<String, Integer>();
    
    @BeforeClass
    public static void before() {
        OpflowLogTracer.addStringifyInterceptor(new StringifyInterceptor() {
            @Override
            public void intercept(Map<String, Object> logdata) {
                if ("Attach method to RpcWorker listener".equals(logdata.get("message"))) {
                    COUNTER.put("routineIds", COUNTER.getOrDefault("routineIds", 0) + 1);
                }
            }
        });
    }
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        COUNTER.clear();
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
    public void test_instantiateType_target_class() throws OpflowBootstrapException, NoSuchMethodException {
        SimpleCalculator calc = OpflowBuilder.createCommander("commander.properties").registerType(SimpleCalculator.class);
        instantiator.instantiateType(SimpleCalculatorImpl.class);
        Assert.assertEquals(COUNTER.get("routineIds").intValue(), SimpleCalculator.class.getDeclaredMethods().length +
                SimpleCalculatorImpl.class.getDeclaredMethods().length);
        System.out.println("COUNTER: " + OpflowJsontool.toString(COUNTER));
        Assert.assertEquals(21, calc.add(20).intValue());
        OpflowRpcResult result = rpcMaster.request("increase", OpflowJsontool.toString(new Object[] {10})).extractResult();
        Assert.assertEquals(11, Integer.parseInt(result.getValueAsString()));
    }
    
    @Test
    public void test_instantiateType_with_inheritance_interface() throws OpflowBootstrapException, NoSuchMethodException {
        instantiator.instantiateType(MirrorCalculator.class, new MirrorCalculatorImpl());
        Assert.assertEquals(COUNTER.get("routineIds").intValue(), MirrorCalculator.class.getDeclaredMethods().length +
                SimpleCalculator.class.getDeclaredMethods().length);
        System.out.println("COUNTER: " + OpflowJsontool.toString(COUNTER));
        OpflowRpcResult result = rpcMaster.request("increase", OpflowJsontool.toString(new Object[] {10})).extractResult();
        Assert.assertEquals(11, Integer.parseInt(result.getValueAsString()));
    }
}
