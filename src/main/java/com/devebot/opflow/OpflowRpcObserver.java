package com.devebot.opflow;

import java.util.Date;

/**
 *
 * @author acegik
 */
public class OpflowRpcObserver {
    
    public interface Listener {
        void check(String componentId);
    }
    
    public static class Manifest {
        private String componentId;
        private Date accessedTime;

        public Manifest(String componentId) {
            this.componentId = componentId;
        }
        
        public void touch() {
            this.accessedTime = new Date();
        }
    }
}
