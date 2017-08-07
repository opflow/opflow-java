package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBroker;
import com.devebot.opflow.exception.OpflowConstructorException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * @author drupalex
 */
public class OpflowBrokerTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructor() throws OpflowConstructorException {
        thrown.expect(OpflowConstructorException.class);
        thrown.expectCause(CoreMatchers.is(IOException.class));
        thrown.expectMessage(CoreMatchers.is("connection refused, invalid connection parameters"));
        Map<String, Object> pars = new HashMap<String, Object>();
        OpflowBroker broker = new OpflowBroker(pars);
    }
    
    @Test
    public void testConstructorWithInvalidExchangeType() throws OpflowConstructorException {
        thrown.expect(OpflowConstructorException.class);
        thrown.expectCause(CoreMatchers.is(IOException.class));
        thrown.expectMessage(CoreMatchers.is("exchangeDeclare has been failed"));
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", "amqp://master:zaq123edcx@192.168.56.56");
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("exchangeType", "nothing");
        OpflowBroker broker = new OpflowBroker(pars);
    }
}
