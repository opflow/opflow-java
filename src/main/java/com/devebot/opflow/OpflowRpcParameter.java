package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Customizer;
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
    
    private String routineId;
    private final String requestId;
    private final String requestTime;
    private final String messageScope;
    private final Boolean callbackTransient;
    private final Boolean progressEnabled;
    private final Boolean watcherEnabled;

    public OpflowRpcParameter(Map<String, Object> options) {
        options = OpflowUtil.ensureNotNull(options);

        this.routineId = OpflowUtil.getRoutineId(options, false);
        this.requestId = OpflowUtil.getRequestId(options);
        this.requestTime = OpflowUtil.getRequestTime(options);
        
        if (options.get("messageScope") instanceof String) {
            this.messageScope = (String) options.get("messageScope");
        } else {
            this.messageScope = null;
        }
        
        this.callbackTransient = "forked".equals((String)options.get("mode"));
        
        if (options.get("progressEnabled") instanceof Boolean) {
            this.progressEnabled = (Boolean) options.get("progressEnabled");
        } else {
            this.progressEnabled = null;
        }
        
        this.watcherEnabled = Boolean.TRUE.equals(options.get("watcherEnabled"));
    }
    
    public OpflowRpcParameter(String requestId, String requestTime, String messageScope, Boolean callbackTransient, Boolean progressEnabled) {
        this.requestId = requestId;
        this.requestTime = requestTime;
        this.messageScope = messageScope;
        this.callbackTransient = callbackTransient;
        this.progressEnabled = progressEnabled;
        this.watcherEnabled = false;
    }

    public String getRoutineId() {
        return routineId;
    }

    public void setRoutineId(String routineId) {
        this.routineId = routineId;
    }
    
    public String getRequestId() {
        return requestId;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public String getMessageScope() {
        return messageScope;
    }
    
    public Boolean getCallbackTransient() {
        return callbackTransient;
    }

    public Boolean getProgressEnabled() {
        return progressEnabled;
    }

    public Boolean getWatcherEnabled() {
        return watcherEnabled;
    }
    
    @Override
    public boolean isMute() {
        return IS_PING_LOGGING_OMITTED && "internal".equals(messageScope);
    }
}
