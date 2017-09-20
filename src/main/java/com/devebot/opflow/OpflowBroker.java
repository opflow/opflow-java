package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.Map;

/**
 *
 * @author drupalex
 * 
 * @deprecated  As of release 0.1.8, replaced by OpflowEngine
 */
@Deprecated
public class OpflowBroker extends OpflowEngine {
    public OpflowBroker(Map<String, Object> params) throws OpflowBootstrapException {
        super(params);
    }
}
