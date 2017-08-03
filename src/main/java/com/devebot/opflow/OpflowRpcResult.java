package com.devebot.opflow;

import java.util.List;

/**
 *
 * @author drupalex
 */
public class OpflowRpcResult {
    private final List<Step> progress;
    private final byte[] error;
    private final byte[] value;
    
    public OpflowRpcResult(List<Step> progress, byte[] error, byte[] value) {
        this.progress = progress;
        this.error = error;
        this.value = value;
    }
    
    public byte[] getError() {
        return error;
    }
    
    public String getErrorAsString() {
        if (error == null) return null;
        return OpflowUtil.getString(error);
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
