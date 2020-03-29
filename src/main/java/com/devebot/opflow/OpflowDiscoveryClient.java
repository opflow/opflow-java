package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.orbitz.consul.nostro.AgentClient;
import com.orbitz.consul.nostro.Consul;
import com.orbitz.consul.nostro.HealthClient;
import com.orbitz.consul.vostro.com.google.common.net.HostAndPort;
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
    
    private final Object healthClientLock = new Object();
    private HealthClient healthClient = null;
    
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
    
    protected HealthClient getHealthClient() {
        if (healthClient == null) {
            synchronized (healthClientLock) {
                if (healthClient == null) {
                    healthClient = getConnection().healthClient();
                }
            }
        }
        return healthClient;
    }
    
    protected void resetConnection() {
        synchronized (connectionLock) {
            if (connection != null) {
                connection.destroy();
                connection = null;
            }
        }
        synchronized (healthClientLock) {
            healthClient = null;
        }
        synchronized (agentClientLock) {
            agentClient = null;
        }
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
}
