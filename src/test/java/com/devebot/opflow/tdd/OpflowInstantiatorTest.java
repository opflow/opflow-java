package com.devebot.opflow.tdd;

import com.devebot.opflow.lab.SimpleCalculatorImpl;
import com.devebot.opflow.lab.SimpleCalculator;
import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowRpcMaster;
import com.devebot.opflow.OpflowRpcResult;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.OpflowUtil;
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
        SimpleCalculator calc = OpflowBuilder.createCommander("commander.properties").registerType(SimpleCalculator.class);
        instantiator.instantiateType(SimpleCalculatorImpl.class);
        Assert.assertEquals(21, calc.add(20).intValue());
        OpflowRpcResult result = rpcMaster.request("increase", OpflowJsontool.toString(new Object[] {10})).extractResult();
        Assert.assertEquals(11, Integer.parseInt(result.getValueAsString()));
    }
}
