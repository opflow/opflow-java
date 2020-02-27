package com.devebot.opflow;

import java.util.Date;

/**
 *
 * @author acegik
 */
public class OpflowRpcObserver {
    
    public interface Listener {
        void check(String componentId, String version, String payload);
    }
    
    public static class Manifest {
        private String componentId;
        private Date accessedTime;
        private Boolean compatible;

        public Manifest(String componentId) {
            this.componentId = componentId;
        }
        
        public void touch() {
            this.accessedTime = new Date();
        }
        
        public void setCompatible(boolean compatible) {
            this.compatible = compatible;
        }
    }
}
