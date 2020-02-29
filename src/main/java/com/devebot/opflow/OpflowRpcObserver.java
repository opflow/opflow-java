package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowDateTime;
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
        private Boolean compatible;
        private final String componentId;
        private final Date originalTime;
        private Date accessedTime;
        private String keepInTouchTime;
        private String losingTouchTime;

        public Manifest(String componentId) {
            this.componentId = componentId;
            this.originalTime = new Date();
        }

        public void touch() {
            this.accessedTime = new Date();
        }

        public void setCompatible(boolean compatible) {
            this.compatible = compatible;
        }

        public Manifest refresh() {
            this.keepInTouchTime = OpflowDateTime.printElapsedTime(originalTime, accessedTime);
            this.losingTouchTime = OpflowDateTime.printElapsedTime(accessedTime, new Date());
            return this;
        }
    }
}
