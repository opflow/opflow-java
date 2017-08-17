package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.exception.OpflowConstructorException;
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
public class OpflowPubsubHandlerTest {
    Properties props;
    
    @Before
    public void beforeEach() throws OpflowConstructorException {
        props = OpflowHelper.loadProperties();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructorWithNullOperatorName() throws OpflowConstructorException {
        thrown.expect(OpflowConstructorException.class);
        thrown.expectMessage(CoreMatchers.is("subscriberName must not be null"));
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.get("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        OpflowPubsubHandler broker = new OpflowPubsubHandler(pars);
    }
}
