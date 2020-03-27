package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryMaster extends OpflowDiscoveryClient {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowDiscoveryWorker.class);
    
    private final String serviceName;
    private final String componentId;
    private final OpflowLogTracer logTracer;
    
    private ServiceHealthCache svHealth;
    private ServiceHealthHook subscriber;
    
    public OpflowDiscoveryMaster(String componentId, String serviceName, Map<String, Object> kwargs) throws OpflowBootstrapException {
        super(kwargs);
        
        this.serviceName = serviceName;
        this.componentId = componentId;
        logTracer = OpflowLogTracer.ROOT.branch("discoveryClientId", this.componentId);
        
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
    
    public void serve() {
        synchronized (this) {
            if (svHealth == null) {
                svHealth = ServiceHealthCache.newCache(getHealthClient(), serviceName);
                svHealth.addListener((Map<ServiceHealthKey, ServiceHealth> newValues) -> {
                    if (subscriber != null) {
                        Map<String, OpflowRpcRoutingInfo> serviceInfo = new LinkedHashMap<>();
                        Set<ServiceHealthKey> keys = newValues.keySet();
                        for (ServiceHealthKey key : keys) {
                            String componentId = key.getServiceId();
                            String hostAndPort = key.getHost() + ":" + key.getPort();
                            OpflowRpcRoutingInfo routingInfo = new OpflowRpcRoutingInfo(OpflowConstant.Protocol.HTTP, componentId, hostAndPort);
                            serviceInfo.put(componentId, routingInfo);
                        }
                        subscriber.onChange(serviceInfo);
                    }
                });
            }
        }
        svHealth.start();
    }
    
    public void close() {
        synchronized (this) {
            if (svHealth != null) {
                svHealth.stop();
                svHealth = null;
            }
        }
    }
    
    public void subscribe(ServiceHealthHook subscriber) {
        this.subscriber = subscriber;
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
    
    public interface ServiceHealthHook {
        void onChange(Map<String, OpflowRpcRoutingInfo> serviceInfo);
    }
}
