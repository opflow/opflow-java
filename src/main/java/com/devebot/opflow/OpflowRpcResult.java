package com.devebot.opflow;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResult implements Serializable {
    private final String routineId;
    private final String requestId;
    private final String workerTag;
    private final List<Step> progress;
    private final boolean failed;
    private final byte[] error;
    private final boolean completed;
    private final byte[] value;
    
    public OpflowRpcResult(String routineId, String requestId, String workerTag, 
            List<Step> progress, 
            boolean failed, byte[] error, 
            boolean completed, byte[] value) {
        this.routineId = routineId;
        this.requestId = requestId;
        this.workerTag = workerTag;
        this.progress = progress;
        this.failed = failed;
        this.error = error;
        this.completed = completed;
        this.value = value;
    }

    public String getRoutineId() {
        return routineId;
    }

    public String getRequestId() {
        return requestId;
    }
    
    public String getWorkerTag() {
        return workerTag;
    }
    
    public boolean isTimeout() {
        return error == null && value == null;
    }
    
    public boolean isFailed() {
        return failed;
    }
    
    public byte[] getError() {
        return error;
    }
    
    public String getErrorAsString() {
        if (error == null) return null;
        return OpflowUtil.getString(error);
    }
    
    public boolean isCompleted() {
        return completed;
    }
    
    public byte[] getValue() {
        return value;
    }
    
    public String getValueAsString() {
        if (value == null) return null;
        return OpflowUtil.getString(value);
    }
    
    public Step[] getProgress() {
        if (progress == null) return null;
        return progress.toArray(new Step[0]);
    }
    
    public static class Step {
        private boolean cracked = false;
        private int percent;
        private String info;
        
        public Step() {
            cracked = true;
        }
        
        public Step(int percent) {
            this(percent, null);
        }
        
        public Step(int percent, String info) {
            this.percent = percent;
            this.info = info;
        }
        
        public int getPercent() {
            return percent;
        }
        
        public String getInfo() {
            return info;
        }

        public boolean isCracked() {
            return cracked;
        }
    }
}
