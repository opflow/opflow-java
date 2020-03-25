package com.devebot.opflow;

import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryWorker {
    private final Consul client;
    private final Object agentClientLock = new Object();
    private AgentClient agentClient = null;
    
    private long interval = 1000L;
    private Timer timer;
    private TimerTask timerTask;
    private volatile boolean running = false;
    private volatile boolean active = true;
    private final Object lock = new Object();
    
    private final String id;
    private final long ttl;
    private String name;
    private Integer port;
    
    public OpflowDiscoveryWorker(String id, long ttl) {
        this.id = id;
        this.ttl = ttl;
        client = Consul.builder().build();
    }
    
    private AgentClient getAgentClient() {
        if (agentClient == null) {
            synchronized (agentClientLock) {
                agentClient = client.agentClient();
            }
        }
        return agentClient;
    }
    
    public void register() {
        if (name == null || port == null) return;
        Registration service = ImmutableRegistration.builder()
            .id(id)
            .name(name)
            .port(port)
            .check(Registration.RegCheck.ttl(ttl))
            .tags(Collections.singletonList("opflow-worker"))
            .meta(Collections.singletonMap("version", "1.0"))
            .build();
        getAgentClient().register(service);
    }
    
    public void beat() throws NotRegisteredException {
        getAgentClient().pass(id);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPort(Integer port) {
        this.port = port;
    }
    
    public boolean isActive() {
        return active;
    }
    
    public void setActive(boolean active) {
        this.active = active;
    }
    
    public synchronized void start() {
        if (!this.running) {
            if (this.timer == null) {
                this.timer = new Timer("Timer-" + OpflowUtil.extractClassName(OpflowDiscoveryWorker.class), true);
            }
            if (this.timerTask == null) {
                this.timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        if (isActive()) {
                            synchronized (lock) {
                                System.out.println("heartbeat!");
                                try {
                                    beat();
                                } catch (NotRegisteredException ex) {
                                    
                                }
                            }
                        }
                    }
                };
            }
            this.timer.scheduleAtFixedRate(this.timerTask, 0, this.interval);
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
            running = false;
        }
        getAgentClient().deregister(id);
    }
}
