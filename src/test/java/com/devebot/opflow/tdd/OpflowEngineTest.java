package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowEngine;
import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * @author drupalex
 */
public class OpflowEngineTest extends OpflowAbstractTest {
    static Properties props;
    
    @BeforeClass
    public static void before() throws OpflowBootstrapException {
        props = OpflowBuilder.loadProperties();
        clearTestExchanges(props.getProperty("opflow.uri"));
        clearTestQueues(props.getProperty("opflow.uri"));
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructor() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(IOException.class));
        thrown.expectMessage(CoreMatchers.is("connection refused, invalid connection parameters"));
        Map<String, Object> pars = new HashMap<String, Object>();
        OpflowEngine engine = new OpflowEngine(pars);
    }
    
    @Test
    public void testConstructorWithInvalidExchangeType() throws OpflowBootstrapException {
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectCause(CoreMatchers.is(IOException.class));
        thrown.expectMessage(CoreMatchers.is("exchangeDeclare has failed"));
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.getProperty("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("exchangeType", "nothing");
        OpflowEngine engine = new OpflowEngine(pars);
    }
}
