package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowDiscoveryClient {

    private final String[] agentHosts;
    
    private final Object connectionLock = new Object();
    private Consul connection;

    private final Object agentClientLock = new Object();
    private AgentClient agentClient = null;
    
    public OpflowDiscoveryClient(Map<String, Object> kwargs) throws OpflowBootstrapException {
        agentHosts = OpflowUtil.getStringArray(kwargs, OpflowConstant.OPFLOW_DISCOVERY_CLIENT_AGENT_HOSTS, null);
        
        try {
            connection = createConnection();
        }
        catch (Exception e) {
            throw new OpflowBootstrapException(e);
        }
    }
    
    protected AgentClient getAgentClient() {
        if (agentClient == null) {
            synchronized (agentClientLock) {
                if (agentClient == null) {
                    agentClient = getConnection().agentClient();
                }
            }
        }
        return agentClient;
    }
    
    protected Consul getConnection() {
        if (connection == null) {
            synchronized (connectionLock) {
                if (connection == null) {
                    try {
                        connection = createConnection();
                    }
                    catch (RuntimeException e) {
                        throw e;
                    }
                }
            }
        }
        return connection;
    }
    
    private Consul createConnection() {
        Consul.Builder builder = Consul.builder();
        
        if (agentHosts != null && agentHosts.length > 0) {
            if (agentHosts.length == 1) {
                builder = builder.withHostAndPort(HostAndPort.fromString(agentHosts[0]));
            } else {
                Collection<HostAndPort> hostAndPorts = new ArrayList<>(agentHosts.length);
                for (String agentHost : agentHosts) {
                    hostAndPorts.add(HostAndPort.fromString(agentHost));
                }
                builder = builder.withMultipleHostAndPort(hostAndPorts, 1000);
            }
        }
        
        return builder.build();
    }
    
    public interface Info {
        String getUri();
        
        default String getVersion() {
            return null;
        }
        
        default Map<String, Object> getOptions() {
            return null;
        }
    }
    
    public interface Locator {
        default boolean available() {
            return true;
        }
        Info locate();
    }
}
