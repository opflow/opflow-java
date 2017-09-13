package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowLoader;
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
        props = OpflowLoader.loadProperties();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructorWithNullConsumerNames() throws OpflowBootstrapException {
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.getProperty("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        OpflowRpcWorker broker = new OpflowRpcWorker(pars);
    }
}
