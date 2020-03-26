package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowObjectTree;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import java.util.Collections;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryWorker {
    private final static long DEFAULT_CHECK_INTERVAL = 2000L; // 2000 milliseconds
    private final static long DEFAULT_CHECK_TTL = 5L; // 5 seconds
    
    private final Consul client;
    private final Object agentClientLock = new Object();
    private AgentClient agentClient = null;
    
    private Timer timer;
    private TimerTask timerTask;
    private volatile boolean running = false;
    private volatile boolean active = true;
    private final Object lock = new Object();
    
    private final String serviceId;
    private final long checkInterval;
    private final long checkTTL;
    private String serviceName;
    private String address;
    private Integer port;
    
    public OpflowDiscoveryWorker(String serviceName, String id, Map<String, Object> kwargs) {
        kwargs = OpflowObjectTree.ensureNonNull(kwargs);
        
        this.serviceName = serviceName;
        this.serviceId = id;
        this.checkInterval = OpflowUtil.getLongField(kwargs, "checkInterval", DEFAULT_CHECK_INTERVAL);
        this.checkTTL = OpflowUtil.getLongField(kwargs, "checkTTL", DEFAULT_CHECK_TTL);
        
        client = Consul.builder().build();
    }
    
    public void setServiceName(String name) {
        this.serviceName = name;
    }

    public OpflowDiscoveryWorker setAddress(String address) {
        this.address = address;
        return this;
    }

    public OpflowDiscoveryWorker setPort(Integer port) {
        this.port = port;
        return this;
    }
    
    public boolean isActive() {
        return active;
    }
    
    public void setActive(boolean active) {
        this.active = active;
    }
    
    public synchronized void start() {
        if (!this.running) {
            this.register();
            if (this.timer == null) {
                this.timer = new Timer("Timer-" + OpflowUtil.extractClassName(OpflowDiscoveryWorker.class), true);
            }
            if (this.timerTask == null) {
                this.timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        if (isActive()) {
                            synchronized (lock) {
                                try {
                                    beat();
                                } catch (NotRegisteredException ex) {
                                    
                                }
                            }
                        }
                    }
                };
            }
            this.timer.scheduleAtFixedRate(this.timerTask, 0, this.checkInterval);
            this.running = true;
        }
    }

    public synchronized void close() {
        if (running) {
            timerTask.cancel();
            timerTask = null;
            timer.cancel();
            timer.purge();
            timer = null;
            deregister();
            running = false;
        }
    }
    
    private void beat() throws NotRegisteredException {
        getAgentClient().pass(serviceId);
    }
    
    private void register() {
        if (serviceName == null || serviceId == null) return;
        ImmutableRegistration.Builder builder = ImmutableRegistration.builder()
            .id(serviceId)
            .name(serviceName);
        
        if (address != null) {
            builder = builder.address(address);
        }
        
        if (port != null) {
            builder = builder.port(port);
        }
        
        builder = builder.check(Registration.RegCheck.ttl(checkTTL))
            .tags(Collections.singletonList("opflow-worker"))
            .meta(Collections.singletonMap("version", "1.0"));
        Registration service = builder.build();
        getAgentClient().register(service);
    }
    
    private void deregister() {
        getAgentClient().deregister(serviceId);
    }
    
    private AgentClient getAgentClient() {
        if (agentClient == null) {
            synchronized (agentClientLock) {
                if (agentClient == null) {
                    agentClient = client.agentClient();
                }
            }
        }
        return agentClient;
    }
}
