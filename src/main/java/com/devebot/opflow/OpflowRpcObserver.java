package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.annotation.OpflowFieldExclude;
import com.devebot.opflow.supports.OpflowConcurrentMap;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowEnvTool;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowRevolvingMap;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowRpcObserver {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    private final static boolean DEBUG = "true".equals(ENVTOOL.getEnvironVariable("OPFLOW_POOR_PERFORMANCE", null));
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcObserver.class);
    
    private final static long KEEP_ALIVE_TIMEOUT = 20000;
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowDiscoveryMaster.ServiceHealthHook serviceUpdater;
    private final OpflowConcurrentMap<String, OpflowRpcObserver.Manifest> manifests = new OpflowConcurrentMap<>();
    private final OpflowRevolvingMap.ChangeListener changeListener = new OpflowRevolvingMap.ChangeListener<String, OpflowRpcRoutingInfo>() {
        @Override
        public OpflowRpcRoutingInfo onUpdating(String key, OpflowRpcRoutingInfo o, OpflowRpcRoutingInfo n) {
            if (DEBUG && LOG.isDebugEnabled()) {
                LOG.debug("onUpdating(" + key + ", " + OpflowJsonTool.toString(o) + ", " + OpflowJsonTool.toString(n));
            }
            return o.update(n);
        }
    };
    private final OpflowRevolvingMap<String, OpflowRpcRoutingInfo> amqpRoutingMap = new OpflowRevolvingMap<>(changeListener);
    private final OpflowRevolvingMap<String, OpflowRpcRoutingInfo> httpRoutingMap = new OpflowRevolvingMap<>(changeListener);
    
    private long keepAliveTimeout = 2 * KEEP_ALIVE_TIMEOUT;
    private boolean congestiveAMQP = false;
    private boolean congestiveHTTP = false;
    
    private final boolean trimmingEnabled;
    private final long trimmingTimeDelay;
    
    private final boolean threadPoolEnabled;
    private final String threadPoolType;
    private final int threadPoolSize;
    private final Object threadExecutorLock = new Object();
    private ExecutorService threadExecutor = null;
    
    public OpflowRpcObserver(Map<String, Object> kwargs) {
        componentId = OpflowUtil.getStringField(kwargs, CONST.COMPONENT_ID, true);
        
        logTracer = OpflowLogTracer.ROOT.branch("rpcCounselorId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcCounselor[${rpcCounselorId}][${instanceId}].new()")
                .stringify());
        
        threadPoolEnabled = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_ENABLED, Boolean.TRUE);
        threadPoolType = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_TYPE, "single");
        threadPoolSize = OpflowUtil.getIntegerField(kwargs, OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_SIZE, 4);
        
        trimmingEnabled = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_ENABLED, Boolean.TRUE);
        trimmingTimeDelay = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_TIME_DELAY, KEEP_ALIVE_TIMEOUT / 10);
        
        serviceUpdater = new OpflowDiscoveryMaster.ServiceHealthHook() {
            @Override
            public void onChange(Map<String, OpflowRpcRoutingInfo> serviceInfo) {
                if (serviceInfo != null) {
                    Set<String> httpRoutingKey = new HashSet<>(httpRoutingMap.keySet());
                    OpflowLogTracer eventTracer = null;
                    if (logTracer.ready(LOG, Level.TRACE)) {
                        eventTracer = logTracer.copy();
                    }
                    if (eventTracer != null && eventTracer.ready(LOG, Level.TRACE)) LOG.trace(eventTracer
                            .put("serviceIds", httpRoutingKey)
                            .text("RpcCounselor[${rpcCounselorId}] current services: ${serviceIds}")
                            .stringify());
                    for (Map.Entry<String, OpflowRpcRoutingInfo> entry : serviceInfo.entrySet()) {
                        String componentId = entry.getKey();
                        httpRoutingMap.put(componentId, entry.getValue());
                        httpRoutingKey.remove(componentId);
                    }
                    if (eventTracer != null && eventTracer.ready(LOG, Level.TRACE)) LOG.trace(eventTracer
                            .put("serviceIds", httpRoutingKey)
                            .text("RpcCounselor[${rpcCounselorId}] removed services: ${serviceIds}")
                            .stringify());
                    httpRoutingMap.removeAll(httpRoutingKey);
                }
            }
        };
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcCounselor[${rpcCounselorId}][${instanceId}].new() end!")
                .stringify());
    }

    public boolean isTrimmingEnabled() {
        return trimmingEnabled;
    }

    public long getTrimmingTimeDelay() {
        return trimmingTimeDelay;
    }

    public boolean isThreadPoolEnabled() {
        return threadPoolEnabled;
    }

    public String getThreadPoolType() {
        return threadPoolType;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public boolean isThreadPoolUsed() {
        return threadExecutor != null;
    }

    public long getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public OpflowDiscoveryMaster.ServiceHealthHook getServiceUpdater() {
        return serviceUpdater;
    }

    public ExecutorService getThreadExecutor() {
        if (threadExecutor == null) {
            synchronized (threadExecutorLock) {
                if (threadExecutor == null) {
                    switch (threadPoolType) {
                        case "cached":
                            threadExecutor = Executors.newCachedThreadPool();
                            break;
                        case "fixed":
                            threadExecutor = Executors.newFixedThreadPool(threadPoolSize);
                            break;
                        default:
                            threadExecutor = Executors.newSingleThreadExecutor();
                            break;
                    }
                }
            }
        }
        return threadExecutor;
    }
    
    public void close() {
        synchronized (threadExecutorLock) {
            if (threadExecutor != null) {
                threadExecutor.shutdown();
                try {
                    if (!threadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                        threadExecutor.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    threadExecutor.shutdownNow();
                }
                finally {
                    threadExecutor = null;
                }
            }
        }
    }
    
    public void check(final OpflowConstant.Protocol protocol, final Map<String, Object> headers) {
        if (!threadPoolEnabled) {
            touch(protocol, headers);
            return;
        }
        getThreadExecutor().submit(new Callable() {
            @Override
            public Object call() throws Exception {
                touch(protocol, headers);
                return null;
            }
        });
    }
    
    public void touch(final OpflowConstant.Protocol protocol, final Map<String, Object> headers) {
        String _componentId = null;
        String httpAddress = null;
        String amqpPattern = null;
        Date currentTimestamp = new Date();
        boolean accepted = false;
        switch (protocol) {
            case AMQP:
                _componentId = OpflowUtil.getStringField(headers, OpflowConstant.OPFLOW_RES_HEADER_SERVERLET_ID, new String[] {CONST.AMQP_HEADER_CONSUMER_ID});
                if (_componentId != null) {
                    OpflowRpcObserver.Manifest manifest = assertManifest(_componentId);
                    accepted = updatingAccepted(manifest.updatedTimestamp, currentTimestamp);
                    if (accepted) {
                        // update the timestamp
                        manifest.updatedTimestamp = manifest.updatedAMQPTimestamp = currentTimestamp;
                        // update the http address
                        httpAddress = OpflowUtil.getStringField(headers, OpflowConstant.OPFLOW_RES_HEADER_HTTP_ADDRESS);
                        if (httpAddress != null) {
                            manifest.httpAddress = httpAddress;
                        }
                        // update the AMQP bindingKey pattern
                        amqpPattern = OpflowUtil.getStringField(headers, OpflowConstant.OPFLOW_RES_HEADER_AMQP_PATTERN);
                        if (amqpPattern != null) {
                            manifest.amqpPattern = amqpPattern;
                        }
                        // update the protocol version
                        String version = OpflowUtil.getStringField(headers, OpflowConstant.OPFLOW_RES_HEADER_PROTO_VERSION);
                        manifest.information.put("AMQP_PROTOCOL_VERSION", version != null ? version : "0");
                    }
                }
                break;
            case HTTP:
                _componentId = OpflowUtil.getStringField(headers, OpflowConstant.OPFLOW_RES_HEADER_SERVERLET_ID);
                if (_componentId != null) {
                    OpflowRpcObserver.Manifest manifest = assertManifest(_componentId);
                    accepted = updatingAccepted(manifest.updatedTimestamp, currentTimestamp);
                    if (accepted) {
                        // update the timestamp
                        manifest.updatedTimestamp = manifest.updatedHTTPTimestamp = currentTimestamp;
                    }
                }
                break;
        }
        // update the routingMap
        if (_componentId != null && accepted) {
            if (amqpPattern != null) {
                amqpRoutingMap.put(_componentId, new OpflowRpcRoutingInfo(OpflowConstant.Protocol.AMQP, _componentId, amqpPattern));
            }
            if (httpAddress != null) {
                httpRoutingMap.put(_componentId, new OpflowRpcRoutingInfo(OpflowConstant.Protocol.HTTP, _componentId, httpAddress));
            }
        }
    }
    
    private boolean updatingAccepted(Date latestTime, Date currentTime) {
        if (!trimmingEnabled) {
            return true;
        }
        if (latestTime == null) {
            return true;
        }
        long diffTime = OpflowDateTime.diffMilliseconds(latestTime, currentTime);
        return diffTime >= trimmingTimeDelay;
    }
    
    public boolean isCongestive() {
        return isCongestive(OpflowConstant.Protocol.AMQP) && isCongestive(OpflowConstant.Protocol.HTTP);
    }
    
    public boolean isCongestive(OpflowConstant.Protocol protocol) {
        switch (protocol) {
            case AMQP:
                return congestiveAMQP;
            case HTTP:
                return congestiveHTTP;
        }
        return false;
    }
    
    public void setCongestive(OpflowConstant.Protocol protocol, boolean congestive) {
        switch (protocol) {
            case AMQP:
                this.congestiveAMQP = congestive;
                break;
            case HTTP:
                this.congestiveHTTP = congestive;
                break;
        }
    }
    
    public void setCongestive(OpflowConstant.Protocol protocol, boolean congestive, String componentId) {
        switch (protocol) {
            case AMQP:
                if (componentId != null) {
                    amqpRoutingMap.get(componentId).setCongestive(congestive);
                }
                break;
            case HTTP:
                if (componentId != null) {
                    httpRoutingMap.get(componentId).setCongestive(congestive);
                }
                break;
        }
    }
    
    public OpflowRpcRoutingInfo getRoutingInfo(OpflowConstant.Protocol protocol) {
        return getRoutingInfo(protocol, true);
    }
    
    public OpflowRpcRoutingInfo getRoutingInfo(OpflowConstant.Protocol protocol, boolean available) {
        switch (protocol) {
            case AMQP:
                if (available) {
                    if (this.congestiveAMQP) {
                        return null;
                    }
                    return selectGoodRoutingInfo(amqpRoutingMap);
                }
                return amqpRoutingMap.rotate();
            case HTTP:
                if (available) {
                    if (this.congestiveHTTP) {
                        return null;
                    }
                    return selectGoodRoutingInfo(httpRoutingMap);
                }
                return httpRoutingMap.rotate();
            default:
                return null;
        }
    }
    
    private OpflowRpcRoutingInfo selectGoodRoutingInfo(OpflowRevolvingMap<String, OpflowRpcRoutingInfo> revolver) {
        OpflowRpcRoutingInfo routingInfo = null;
        int size = revolver.size();
        while (size > 0) {
            OpflowRpcRoutingInfo info = revolver.rotate();
            if (!info.isCongestive()) {
                if (DEBUG && LOG.isDebugEnabled()) {
                    LOG.debug("Ok");
                }
                routingInfo = info;
                break;
            }
            size--;
        }
        if (routingInfo == null) {
            if (DEBUG && LOG.isDebugEnabled()) {
                LOG.debug("Not found");
            }
        }
        return routingInfo;
    }
    
    public boolean containsInfo(String componentId, String name) {
        if (componentId == null) return false;
        OpflowRpcObserver.Manifest manifest = manifests.get(componentId);
        if (manifest == null) return false;
        return (manifest.information.containsKey(name));
    }
    
    public void updateInfo(String componentId, String name, Object data) {
        if (componentId == null) return;
        OpflowRpcObserver.Manifest manifest = manifests.get(componentId);
        if (manifest != null) {
            synchronized (manifest.information) {
                manifest.information.put(name, data);
            }
        }
    }
    
    public void cleanup() {
        Set<String> keys = manifests.keySet();
        for (String key: keys) {
            OpflowRpcObserver.Manifest manifest = manifests.get(key);
            // refresh the state of the manifest
            manifest.refresh(keepAliveTimeout);
            // filter the active manifests
            if (manifest.expired()) {
                manifests.remove(key);
                amqpRoutingMap.remove(key);
                httpRoutingMap.remove(key);
            }
        }
    }
    
    public Collection<OpflowRpcObserver.Manifest> summary() {
        this.cleanup();
        return manifests.values();
    }
    
    public void setKeepAliveTimeout(long timeout) {
        if (timeout > 0) {
            this.keepAliveTimeout = KEEP_ALIVE_TIMEOUT + Long.min(KEEP_ALIVE_TIMEOUT, timeout);
        }
    }
    
    private Manifest assertManifest(String componentId) {
        OpflowRpcObserver.Manifest manifest = manifests.get(componentId);
        if (manifest == null) {
            manifest = new OpflowRpcObserver.Manifest(componentId);
            manifests.put(componentId, manifest);
        }
        return manifest;
    }
    
    public static class Manifest {
        public final static String STATUS_OK = "green";
        public final static String STATUS_ABSENT = "yellow";
        public final static String STATUS_BROKEN = "red";
        public final static String STATUS_CUTOFF = "gray";

        private String status;
        private String httpAddress;
        private String amqpPattern;
        private Boolean compatible;
        private final String componentId;
        private final Map<String, Object> information;
        private final Date reachedTimestamp;
        private Date updatedTimestamp;
        private Date updatedAMQPTimestamp;
        private Date updatedHTTPTimestamp;

        @OpflowFieldExclude
        private long keepInTouchDuration;
        private String keepInTouchTime;
        @OpflowFieldExclude
        private long losingTouchDuration;
        private String losingTouchTime;

        public Manifest(String componentId) {
            this.componentId = componentId;
            this.information = new LinkedHashMap<>();
            this.reachedTimestamp = new Date();
        }

        public void touch() {
            this.updatedTimestamp = new Date();
        }
        
        public Manifest refresh(long keepAliveTimeout) {
            // update the timestamps
            this.keepInTouchDuration = updatedTimestamp.getTime() - reachedTimestamp.getTime();
            this.keepInTouchTime = OpflowDateTime.printElapsedTime(this.keepInTouchDuration);
            this.losingTouchDuration = (new Date()).getTime() - updatedTimestamp.getTime();
            this.losingTouchTime = OpflowDateTime.printElapsedTime(this.losingTouchDuration);
            // update the compatible field
            if (compatible == null) {
                Object version = information.get("AMQP_PROTOCOL_VERSION");
                if (version != null) {
                    if (version.equals(CONST.AMQP_PROTOCOL_VERSION)) {
                        this.compatible = true;
                    } else {
                        if (version.equals("0")) {
                            this.compatible = CONST.LEGACY_HEADER_ENABLED;
                        } else {
                            this.compatible = false;
                        }
                    }
                }
            }
            // update the status of the manifest
            if (losingTouchDuration > keepAliveTimeout * 3) {
                this.status = STATUS_CUTOFF;
            } else if (losingTouchDuration > keepAliveTimeout * 2) {
                this.status = STATUS_BROKEN;
            } else if (losingTouchDuration > keepAliveTimeout) {
                this.status = STATUS_ABSENT;
            } else {
                this.status = STATUS_OK;
            }
            return this;
        }
        
        public boolean expired() {
            return STATUS_CUTOFF.equals(this.status);
        }
    }
}
