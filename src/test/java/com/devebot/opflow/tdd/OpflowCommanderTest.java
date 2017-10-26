package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowLogTracer;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowRpcListener;
import com.devebot.opflow.OpflowRpcResponse;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.lab.MirrorCalculator;
import com.devebot.opflow.lab.SimpleCalculator;
import com.devebot.opflow.lab.SimpleCalculatorException;
import com.devebot.opflow.lab.SimpleCalculatorImpl;
import java.io.IOException;
import java.util.Map;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 *
 * @author drupalex
 */
public class OpflowCommanderTest {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowCommanderTest.class);
    private OpflowRpcWorker rpcWorker;
    private OpflowServerlet.Instantiator instantiator;
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        OpflowLogTracer.clearStringifyInterceptors();
        rpcWorker = OpflowBuilder.createRpcWorker();
        instantiator = new OpflowServerlet.Instantiator(rpcWorker, OpflowUtil.buildMap()
                .put("autorun", true).toMap());
    }
    
    @After
    public void afterEach() {
        if (rpcWorker != null) rpcWorker.close();
        OpflowLogTracer.clearStringifyInterceptors();
    }
    
    @Test
    public void testMassCallingMethods() throws OpflowBootstrapException {
        SimpleCalculator calc = OpflowBuilder.createCommander("commander.properties").registerType(SimpleCalculator.class);
        instantiator.instantiateType(SimpleCalculatorImpl.class);
        for(int k=0; k<100; k++) {
            // System.out.println("Result: " + calc.add(100, k));
            assertThat(calc.add(100, k), equalTo(100+k));
        }
        calc.printInfo();
    }
    
    @Test
    public void testThrowException() throws SimpleCalculatorException, OpflowBootstrapException {
        SimpleCalculator calc = OpflowBuilder.createCommander("commander.properties").registerType(SimpleCalculator.class);
        instantiator.instantiateType(SimpleCalculatorImpl.class);
        thrown.expect(SimpleCalculatorException.class);
        thrown.expectMessage(CoreMatchers.is("this is a demo"));
        try {
            assertThat(calc.tick(), equalTo(1));
            assertThat(calc.tick(), not(equalTo(2)));
        } catch (SimpleCalculatorException exception) {
            //System.out.println("Exception: " + exception.getClass().getName() + "/" + exception.getMessage());
            //exception.printStackTrace();
            throw exception;
        }
    }
    
    @Test
    public void test_monitor_configuration() throws OpflowBootstrapException {
        OpflowLogTracer.addStringifyInterceptor(new OpflowLogTracer.StringifyInterceptor() {
            @Override
            public void intercept(Map<String, Object> logdata) {
                if ("RpcMaster.new() parameters".equals(logdata.get("message"))) {
                    System.out.println("Log object: " + OpflowJsontool.toString(logdata));
                    assertThat((Boolean)logdata.get("monitorEnabled"), equalTo(Boolean.TRUE));
                    assertThat((Integer)logdata.get("monitorInterval"), equalTo(1500));
                    assertThat((Long)logdata.get("monitorTimeout"), equalTo(4000l));
                }
            }
        });
        OpflowBuilder.createCommander("commander_waiting.properties");
    }
    
    @Test
    public void test_() throws OpflowBootstrapException {
        rpcWorker.process("add_by_1", new OpflowRpcListener() {
            @Override
            public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                try {
                    String msg = message.getBodyAsString();
                    response.emitCompleted("100");
                } catch (final Exception ex) {
                    String errmsg = OpflowUtil.buildMap()
                            .put("exceptionClass", ex.getClass().getName())
                            .put("exceptionMessage", ex.getMessage())
                            .toString();
                    response.emitFailed(errmsg);
                }
                return null;
            }
        });
        MirrorCalculator calc = OpflowBuilder.createCommander("commander.properties").registerType(MirrorCalculator.class);
        assertThat(calc.add(99), equalTo(100));
    }
}
