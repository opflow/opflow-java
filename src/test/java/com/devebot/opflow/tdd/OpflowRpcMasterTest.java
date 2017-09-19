package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowRpcListener;
import com.devebot.opflow.OpflowRpcMaster;
import com.devebot.opflow.OpflowRpcResponse;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowConsumerOverLimitException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * @author drupalex
 */
public class OpflowRpcMasterTest {
    Properties props;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        props = OpflowLoader.loadProperties();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testExceedingLimitResponse() throws OpflowBootstrapException {
        thrown.expect(OpflowConsumerOverLimitException.class);
        thrown.expectMessage(CoreMatchers.startsWith("consumerLimit exceed"));
        
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.getProperty("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        pars.put("operatorName", "tdd-opflow-queue");
        pars.put("responseName", "tdd-opflow-feedback");
        OpflowRpcWorker broker = new OpflowRpcWorker(pars);
        broker.process(new OpflowRpcListener() {
            @Override
            public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                return null;
            }
        });
        
        Map<String, Object> par1 = new HashMap<String, Object>();
        par1.put("uri", props.getProperty("opflow.uri"));
        par1.put("exchangeName", "tdd-opflow-exchange");
        par1.put("routingKey", "tdd-opflow-rpc");
        par1.put("responseName", "tdd-opflow-feedback");
        String input = OpflowUtil.buildMap().put("number", 25).toString();
        OpflowRpcMaster m1 = new OpflowRpcMaster(par1);
        OpflowRpcMaster m2 = new OpflowRpcMaster(par1);
        m1.request("fib", input);
        m2.request("fib", input);
    }
}
