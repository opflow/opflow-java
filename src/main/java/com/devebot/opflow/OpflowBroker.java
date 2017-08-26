package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowBroker extends OpflowEngine {
    public OpflowBroker(Map<String, Object> params) throws OpflowBootstrapException {
        super(params);
    }
}
