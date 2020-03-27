package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryMaster extends OpflowDiscoveryClient {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowDiscoveryWorker.class);
    
    private final OpflowLogTracer logTracer;
    
    public OpflowDiscoveryMaster(String serviceId, Map<String, Object> kwargs) throws OpflowBootstrapException {
        super(kwargs);
        
        logTracer = OpflowLogTracer.ROOT.branch("discoveryClientId", serviceId);
        
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("DiscoveryMaster[${discoveryClientId}][${instanceId}].new()")
                .stringify());
        }
        
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("DiscoveryMaster[${discoveryClientId}][${instanceId}].new() end!")
                .stringify());
        }
    }
}
