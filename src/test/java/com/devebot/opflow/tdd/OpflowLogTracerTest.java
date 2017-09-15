package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowLogTracer;
import com.google.gson.Gson;
import java.util.HashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowLogTracerTest {
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowLogTracerTest.class);
    
    @Test
    public void testCopyMethod() {
        
        
        OpflowLogTracer logTracer = new OpflowLogTracer();
        logTracer.put("instanceId", "demo");
        logTracer.put("rootId", "1234567890");
        
        String empty = logTracer.copy()
                .toString();
        
        assertThat(empty, equalTo("{\"instanceId\":\"demo\",\"rootId\":\"1234567890\"}"));
        
        String logmsg = logTracer.copy()
                .put("message", "Hello Opflow")
                .toString();
        
        assertThat(logmsg, equalTo("{\"message\":\"Hello Opflow\",\"instanceId\":\"demo\",\"rootId\":\"1234567890\"}"));
        
        OpflowLogTracer subTracer1 = logTracer.copy()
                .put("appId", "app1");
        
        OpflowLogTracer subTracer2 = logTracer.copy()
                .put("appId", "app2");
        
        String logmsg1 = subTracer1
                .put("message", "Hello App1")
                .toString();
        
        String logmsg2 = subTracer2
                .put("message", "Hello App2")
                .toString();
        
        assertThat(logmsg1, equalTo("{\"message\":\"Hello App1\",\"instanceId\":\"demo\",\"rootId\":\"1234567890\",\"appId\":\"app1\"}"));
        assertThat(logmsg2, equalTo("{\"message\":\"Hello App2\",\"instanceId\":\"demo\",\"rootId\":\"1234567890\",\"appId\":\"app2\"}"));
        
        LOG.debug("logmsg1: " + logmsg1);
        LOG.debug("logmsg2: " + logmsg2);
    }
    
    @Test
    public void testGsonToJsonMethod() {
        Gson gson = new Gson();
        OpflowLogTracer logObject = new OpflowLogTracer();
        
        logObject.put("engineId", "engine1").put("consumerId", "consumer1");
        
        HashMap<String, Object> A = new HashMap<String, Object>();
        
        A.put("A1", "Value 1");
        A.put("A2", "Value 2");
        
        HashMap<String, Object> A2 = new HashMap<String, Object>();
        
        A2.put("A2_1", "Sub 1");
        A2.put("A2_2", "Sub 2");
        
        A.put("A2", A2);
        
        HashMap<String, String> B = (HashMap<String, String>) A.clone();
        
        A2.put("A2_2", "Sub 9");
        
        LOG.debug("Json string: " + gson.toJson(B));
    }
}
