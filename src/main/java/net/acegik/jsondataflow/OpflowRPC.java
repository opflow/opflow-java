package net.acegik.jsondataflow;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRPC {

    final Logger logger = LoggerFactory.getLogger(OpflowRPC.class);

    private OpflowEngine engine;

    public OpflowRPC(Map<String, Object> params) throws Exception {
        engine = new OpflowEngine(params);
    }

    public void process(OpflowChangeListener listener) {
        engine.consume(listener);
    }

    public void close() {
        if (engine != null) engine.close();
    }
}