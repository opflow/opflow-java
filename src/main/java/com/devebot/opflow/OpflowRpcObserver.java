package com.devebot.opflow;

import com.devebot.opflow.annotation.OpflowFieldExclude;
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
        private final Date startedTime;
        private Date updatedTime;

        @OpflowFieldExclude
        private long keepInTouchDuration;
        private String keepInTouchTime;
        @OpflowFieldExclude
        private long losingTouchDuration;
        private String losingTouchTime;

        public Manifest(String componentId) {
            this.componentId = componentId;
            this.startedTime = new Date();
        }

        public void touch() {
            this.updatedTime = new Date();
        }

        public void setCompatible(boolean compatible) {
            this.compatible = compatible;
        }

        public long getLosingTouchDuration() {
            return losingTouchDuration;
        }
        
        public Manifest refresh() {
            this.keepInTouchDuration = updatedTime.getTime() - startedTime.getTime();
            this.keepInTouchTime = OpflowDateTime.printElapsedTime(this.keepInTouchDuration);
            this.losingTouchDuration = (new Date()).getTime() - updatedTime.getTime();
            this.losingTouchTime = OpflowDateTime.printElapsedTime(this.losingTouchDuration);
            return this;
        }
    }
}
