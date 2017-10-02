package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * @author drupalex
 */
public class OpflowRpcWorkerTest {
    Properties props;
    Map<String, Object> pars;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        props = OpflowBuilder.loadProperties();
        pars = new HashMap<String, Object>();
        pars.put("uri", props.getProperty("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructorWithNullConsumerNames() throws OpflowBootstrapException {
        OpflowRpcWorker rpcWorker = new OpflowRpcWorker(pars);
        rpcWorker.close();
    }
}
