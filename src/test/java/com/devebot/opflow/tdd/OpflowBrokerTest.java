package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowEngine;
import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.exception.OpflowConstructorException;
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
public class OpflowBrokerTest {
    Properties props;
    
    @Before
    public void beforeEach() throws OpflowConstructorException {
        props = OpflowHelper.loadProperties();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructor() throws OpflowConstructorException {
        thrown.expect(OpflowConstructorException.class);
        thrown.expectCause(CoreMatchers.is(IOException.class));
        thrown.expectMessage(CoreMatchers.is("connection refused, invalid connection parameters"));
        Map<String, Object> pars = new HashMap<String, Object>();
        OpflowEngine engine = new OpflowEngine(pars);
    }
    
    @Test
    public void testConstructorWithInvalidExchangeType() throws OpflowConstructorException {
        thrown.expect(OpflowConstructorException.class);
        thrown.expectCause(CoreMatchers.is(IOException.class));
        thrown.expectMessage(CoreMatchers.is("exchangeDeclare has been failed"));
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.get("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("exchangeType", "nothing");
        OpflowEngine engine = new OpflowEngine(pars);
    }
}
