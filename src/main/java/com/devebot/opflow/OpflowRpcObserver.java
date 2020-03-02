package com.devebot.opflow;

import com.devebot.opflow.annotation.OpflowFieldExclude;
import com.devebot.opflow.supports.OpflowConcurrentMap;
import com.devebot.opflow.supports.OpflowDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

/**
 *
 * @author acegik
 */
public class OpflowRpcObserver {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();

    private final static long KEEP_ALIVE_TIMEOUT = 20000;

    private long keepAliveTimeout = 2 * KEEP_ALIVE_TIMEOUT;
    private final OpflowConcurrentMap<String, OpflowRpcObserver.Manifest> manifests = new OpflowConcurrentMap<>();

    public void check(String componentId, String version, String payload) {
        OpflowRpcObserver.Manifest manifest = null;
        if (componentId != null) {
            // assure the manifest object
            if (manifests.containsKey(componentId)) {
                manifest = manifests.get(componentId);
            } else {
                manifest = new OpflowRpcObserver.Manifest(componentId);
                manifests.put(componentId, manifest);
            }
            manifest.touch();
            // update the compatible status
            if (version == null) {
                manifest.setCompatible(CONST.LEGACY_HEADER_ENABLED);
            } else {
                if (version.equals(CONST.AMQP_PROTOCOL_VERSION)) {
                    manifest.setCompatible(true);
                } else {
                    manifest.setCompatible((version.equals("0") && CONST.LEGACY_HEADER_ENABLED));
                }
            }
        }
    }

    public Collection<OpflowRpcObserver.Manifest> rollup() {
        Set<String> keys = manifests.keySet();
        for (String key: keys) {
            // refresh the state of the manifest
            OpflowRpcObserver.Manifest manifest = manifests.get(key);
            manifest.refresh();
            // validate the state of the manifest
            if (manifest.getLosingTouchDuration() > keepAliveTimeout) {
                manifests.remove(key);
            }
        }
        return manifests.values();
    }

    public Object getInformation() {
        return this.rollup();
    }

    public void setKeepAliveTimeout(long timeout) {
        if (timeout > 0) {
            this.keepAliveTimeout = KEEP_ALIVE_TIMEOUT + Long.min(KEEP_ALIVE_TIMEOUT, timeout);
        }
    }

    public static class Manifest {
        private Boolean compatible;
        private final String componentId;
        private final Date reachedTimestamp;
        private Date updatedTimestamp;

        @OpflowFieldExclude
        private long keepInTouchDuration;
        private String keepInTouchTime;
        @OpflowFieldExclude
        private long losingTouchDuration;
        private String losingTouchTime;

        public Manifest(String componentId) {
            this.componentId = componentId;
            this.reachedTimestamp = new Date();
        }

        public void touch() {
            this.updatedTimestamp = new Date();
        }

        public void setCompatible(boolean compatible) {
            this.compatible = compatible;
        }

        public long getLosingTouchDuration() {
            return losingTouchDuration;
        }
        
        public Manifest refresh() {
            this.keepInTouchDuration = updatedTimestamp.getTime() - reachedTimestamp.getTime();
            this.keepInTouchTime = OpflowDateTime.printElapsedTime(this.keepInTouchDuration);
            this.losingTouchDuration = (new Date()).getTime() - updatedTimestamp.getTime();
            this.losingTouchTime = OpflowDateTime.printElapsedTime(this.losingTouchDuration);
            return this;
        }
    }
}
