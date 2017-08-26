package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.exception.OpflowBootstrapException;
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
public class OpflowRpcWorkerTest {
    Properties props;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        props = OpflowHelper.loadProperties();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructorWithNullOperatorName() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectMessage(CoreMatchers.is("operatorName must not be null"));
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.getProperty("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        OpflowRpcWorker broker = new OpflowRpcWorker(pars);
    }
}