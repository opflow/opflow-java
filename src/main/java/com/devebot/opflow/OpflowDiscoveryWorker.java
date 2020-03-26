package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryWorker {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowDiscoveryWorker.class);
    
    private final static long DEFAULT_CHECK_INTERVAL = 2000L; // 2000 milliseconds
    private final static long DEFAULT_CHECK_TTL = 5L; // 5 seconds
    
    private final OpflowLogTracer logTracer;
    private final Consul client;
    private final Object agentClientLock = new Object();
    private AgentClient agentClient = null;
    
    private Timer timer;
    private TimerTask timerTask;
    private volatile boolean running = false;
    private volatile boolean active = true;
    private final Object lock = new Object();
    
    private final String serviceName;
    private final String serviceId;
    private final long checkInterval;
    private final long checkTTL;
    
    private String address;
    private Integer port;
    
    public OpflowDiscoveryWorker(String serviceName, String serviceId, Map<String, Object> kwargs) {
        this.serviceName = serviceName;
        this.serviceId = serviceId;
        this.logTracer = OpflowLogTracer.ROOT.branch("discoveryWorkerId", serviceId);

        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("DiscoveryWorker[${discoveryWorkerId}][${instanceId}].new()")
                .stringify());
        }
        
        this.checkInterval = OpflowUtil.getLongField(kwargs, "checkInterval", DEFAULT_CHECK_INTERVAL);
        this.checkTTL = OpflowUtil.getLongField(kwargs, "checkTTL", DEFAULT_CHECK_TTL);
        
        client = Consul.builder().build();
        
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("DiscoveryWorker[${discoveryWorkerId}][${instanceId}].new() end!")
                .stringify());
        }
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
            // register the service
            this.register();
            // create the timer
            if (this.timer == null) {
                this.timer = new Timer("Timer-" + OpflowUtil.extractClassName(OpflowDiscoveryWorker.class), true);
            }
            // create the task
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
            // scheduling the task
            this.timer.scheduleAtFixedRate(this.timerTask, 0, this.checkInterval);
            // turned-on
            this.running = true;
        }
    }

    public synchronized void close() {
        if (running) {
            // cancel the task
            timerTask.cancel();
            timerTask = null;
            // cancel the timer
            timer.cancel();
            timer.purge();
            timer = null;
            // deregister the service
            deregister();
            // turned-off
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
        
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .put("serviceName", serviceName)
                .put("serviceId", serviceId)
                .put("address", address)
                .put("port", port)
                .text("DiscoveryWorker[${discoveryWorkerId}] - register the service[${serviceName}][${serviceId}] with the address[${address}:${port}]")
                .stringify());
        }
        getAgentClient().register(service);
    }
    
    private void deregister() {
        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .put("serviceName", serviceName)
                .put("serviceId", serviceId)
                .text("DiscoveryWorker[${discoveryWorkerId}] - deregister the service[${serviceName}][${serviceId}]")
                .stringify());
        }
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
