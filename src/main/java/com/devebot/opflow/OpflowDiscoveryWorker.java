package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.AgentClient;
import com.orbitz.consul.Consul;
import com.orbitz.consul.NotRegisteredException;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
    private final static long DEFAULT_CHECK_TTL = 5000L; // 5000 milliseconds
    
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
    private String version;
    private String[] tags;
    
    public OpflowDiscoveryWorker(String serviceName, String serviceId, Map<String, Object> kwargs) throws OpflowBootstrapException {
        this.serviceName = serviceName;
        this.serviceId = serviceId;
        this.logTracer = OpflowLogTracer.ROOT.branch("discoveryWorkerId", serviceId);

        if (logTracer.ready(LOG, Level.INFO)) {
            LOG.info(logTracer
                .text("DiscoveryWorker[${discoveryWorkerId}][${instanceId}].new()")
                .stringify());
        }
        
        long _checkInterval = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_DISCOVERY_CLIENT_CHECK_INTERVAL, DEFAULT_CHECK_INTERVAL);
        long _checkTTL = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_DISCOVERY_CLIENT_CHECK_TTL, DEFAULT_CHECK_TTL);
        
        if (_checkInterval <= 1000) {
            _checkInterval = 1000;
        }
        
        if (_checkTTL <= 1050) {
            _checkTTL = 1050;
        }
        
        if (_checkTTL < _checkInterval + 50) {
            _checkTTL = 2 * _checkInterval;
        }
        
        this.checkInterval = _checkInterval;
        this.checkTTL = (_checkTTL + 999l) / 1000l;
        
        if (logTracer.ready(LOG, Level.DEBUG)) {
            LOG.debug(logTracer
                .put("checkInterval", checkInterval)
                .put("checkTTL", checkTTL)
                .text("DiscoveryWorker[${discoveryWorkerId}][${instanceId}].new() - checking interval [${checkInterval}](ms) and ttl [${checkTTL}](s)")
                .stringify());
        }
        
        // Build the connection
        Consul.Builder builder = Consul.builder();
        
        String[] agentHosts = OpflowUtil.getStringArray(kwargs, OpflowConstant.OPFLOW_DISCOVERY_CLIENT_AGENT_HOSTS, null);
        
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
        
        try {
            client = builder.build();
        }
        catch (Exception e) {
            throw new OpflowBootstrapException(e);
        }
        
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

    public void setVersion(String version) {
        this.version = version;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }
    
    public boolean isActive() {
        return active;
    }
    
    public void setActive(boolean active) {
        this.active = active;
    }
    
    public synchronized void start() {
        if (!this.running) {
            if (logTracer.ready(LOG, Level.INFO)) {
                LOG.info(logTracer
                    .text("DiscoveryWorker[${discoveryWorkerId}] start a Service Check Timer")
                    .stringify());
            }
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
        } else {
            if (logTracer.ready(LOG, Level.TRACE)) {
                LOG.trace(logTracer
                    .text("DiscoveryWorker[${discoveryWorkerId}] Service Check Timer has already started")
                    .stringify());
            }
        }
    }

    public synchronized void close() {
        if (running) {
            if (logTracer.ready(LOG, Level.INFO)) {
                LOG.info(logTracer
                    .text("DiscoveryWorker[${discoveryWorkerId}] stop the Service Check Timer")
                    .stringify());
            }
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
        } else {
            if (logTracer.ready(LOG, Level.TRACE)) {
                LOG.trace(logTracer
                    .text("DiscoveryWorker[${discoveryWorkerId}] Service Check Timer has already stopped")
                    .stringify());
            }
        }
    }
    
    private void beat() throws NotRegisteredException {
        getAgentClient().pass(serviceId);
    }
    
    private void register() {
        if (serviceName == null || serviceId == null) return;
        ImmutableRegistration.Builder builder = ImmutableRegistration.builder()
            .id(serviceId)
            .name(serviceName)
            .check(Registration.RegCheck.ttl(checkTTL));
        
        if (address != null) {
            builder = builder.address(address);
        }
        
        if (port != null) {
            builder = builder.port(port);
        }
        
        if (version != null) {
            builder = builder.meta(Collections.singletonMap("version", version));
        }
        
        if (tags != null && tags.length > 0) {
            builder = builder.tags(Arrays.asList(tags));
        }
        
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
