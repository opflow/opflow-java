package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryMaster extends OpflowDiscoveryClient {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowDiscoveryWorker.class);
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    
    public OpflowDiscoveryMaster(String serviceId, Map<String, Object> kwargs) throws OpflowBootstrapException {
        super(kwargs);
        
        componentId = serviceId;
        logTracer = OpflowLogTracer.ROOT.branch("discoveryClientId", componentId);
        
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

    public String getComponentId() {
        return componentId;
    }
    
    public List<Map<String, Object>> getService(String workerName) {
        List<Map<String, Object>> result = new LinkedList<>();
        List<ServiceHealth> nodes = getHealthClient().getHealthyServiceInstances(workerName).getResponse();
        for (ServiceHealth node : nodes) {
            Service service = node.getService();
            if (service != null) {
                result.add(OpflowObjectTree.buildMap()
                    .put("hostname", service.getAddress())
                    .put("port", service.getPort())
                    .toMap());
            }
        }
        return result;
    }
}
