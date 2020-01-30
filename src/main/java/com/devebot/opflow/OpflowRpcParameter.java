package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Customizer;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowEnvTool;
import java.util.Map;

/**
 *
 * @author acegik
 */
public class OpflowRpcParameter implements Customizer {
    private final static boolean IS_PING_LOGGING_OMITTED;
    
    static {
        IS_PING_LOGGING_OMITTED = !"false".equals(OpflowEnvTool.instance.getSystemProperty("OPFLOW_OMIT_PING_LOGS", null));
    }
    
    private final String requestId;
    private final String requestTime;
    private Long requestTTL = null;
    private String routineId = null;
    private String messageScope = null;
    private Boolean callbackTransient = false;
    private Boolean progressEnabled = null;
    private Boolean watcherEnabled = false;

    public OpflowRpcParameter() {
        this.requestId = OpflowUUID.getBase64ID();
        this.requestTime = OpflowDateTime.getCurrentTimeString();
    }
    
    public OpflowRpcParameter(Map<String, Object> options) {
        options = OpflowUtil.ensureNotNull(options);

        this.routineId = OpflowUtil.getRoutineId(options, false);
        this.requestId = OpflowUtil.getRequestId(options);
        this.requestTime = OpflowUtil.getRequestTime(options);
        
        if (options.get("timeout") instanceof Long) {
            this.requestTTL = (Long) options.get("timeout");
        }
        
        if (options.get("messageScope") instanceof String) {
            this.messageScope = (String) options.get("messageScope");
        }
        
        this.callbackTransient = "forked".equals((String)options.get("mode"));
        
        if (options.get("progressEnabled") instanceof Boolean) {
            this.progressEnabled = (Boolean) options.get("progressEnabled");
        }
        
        this.watcherEnabled = Boolean.TRUE.equals(options.get("watcherEnabled"));
    }
    
    public OpflowRpcParameter(String requestId, String requestTime) {
        this.requestId = requestId;
        this.requestTime = requestTime;
    }

    public String getRoutineId() {
        return routineId;
    }

    public OpflowRpcParameter setRoutineId(String routineId) {
        this.routineId = routineId;
        return this;
    }
    
    public String getRequestId() {
        return requestId;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public Long getRequestTTL() {
        return requestTTL;
    }

    public OpflowRpcParameter setRequestTTL(Long requestTTL) {
        this.requestTTL = requestTTL;
        return this;
    }
    
    public String getMessageScope() {
        return messageScope;
    }
    
    public OpflowRpcParameter setMessageScope(String messageScope) {
        this.messageScope = messageScope;
        return this;
    }
    
    public Boolean getCallbackTransient() {
        return callbackTransient;
    }

    public OpflowRpcParameter setCallbackTransient(Boolean callbackTransient) {
        this.callbackTransient = callbackTransient;
        return this;
    }

    public Boolean getProgressEnabled() {
        return progressEnabled;
    }

    public OpflowRpcParameter setProgressEnabled(Boolean progressEnabled) {
        this.progressEnabled = progressEnabled;
        return this;
    }

    public Boolean getWatcherEnabled() {
        return watcherEnabled;
    }
    
    public OpflowRpcParameter setWatcherEnabled(Boolean watcherEnabled) {
        this.watcherEnabled = watcherEnabled;
        return this;
    }
    
    @Override
    public boolean isMute() {
        return IS_PING_LOGGING_OMITTED && "internal".equals(messageScope);
    }
}
