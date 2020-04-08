package com.devebot.opflow;

import com.devebot.opflow.annotation.OpflowSourceRoutine;
import com.devebot.opflow.exception.OpflowMethodNotFoundException;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowRpcChecker {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    private final static String[] REQUEST_ATTRS = new String[] {
        OpflowConstant.OPFLOW_COMMON_PROTOCOL,
        OpflowConstant.ROUTINE_ID,
        OpflowConstant.OPFLOW_COMMON_START_TIMESTAMP,
        OpflowConstant.OPFLOW_COMMON_END_TIMESTAMP,
        OpflowConstant.OPFLOW_COMMON_ELAPSED_TIME,
    };
    
    @OpflowSourceRoutine(alias = OpflowConstant.OPFLOW_ROUTINE_PINGPONG_ALIAS)
    public abstract Pong send(Ping info) throws Throwable;
    
    private static String sendMethodName = null;
    
    public static String getSendMethodName() throws NoSuchMethodException {
        if (sendMethodName == null) {
            synchronized (OpflowRpcChecker.class) {
                if (sendMethodName == null) {
                    Method method = OpflowRpcChecker.class.getMethod("send", Ping.class);
                    String alias = null;
                    OpflowSourceRoutine routine = OpflowUtil.extractMethodAnnotation(method, OpflowSourceRoutine.class);
                    if (routine != null) {
                        alias = routine.alias();
                    }
                    if (CONST.LEGACY_ROUTINE_PINGPONG_APPLIED || alias == null || alias.isEmpty()) {
                        sendMethodName = method.toString();
                    } else {
                        sendMethodName = alias;
                    }
                }
            }
        }
        return sendMethodName;
    }
    
    public static String getPingSignature() {
        try {
            return getSendMethodName();
        } catch (NoSuchMethodException exception) {
            throw new OpflowMethodNotFoundException(exception);
        }
    }
    
    public static class Ping {
        String q;

        public Ping() {}
        
        public Ping(String q) {
            this.q = q;
        }

        public String getQ() {
            return q;
        }

        public void setQ(String q) {
            this.q = q;
        }
    }
    
    public static class Pong {
        private Map<String, Object> parameters;
        private Map<String, Object> accumulator;

        public Pong() {
        }
        
        public Pong(Map<String, Object> accumulator) {
            this.accumulator = accumulator;
        }

        public Map<String, Object> getParameters() {
            if (parameters == null) {
                parameters = new LinkedHashMap<>();
            }
            return parameters;
        }

        public void setParameters(Map<String, Object> parameters) {
            this.parameters = parameters;
        }

        public Map<String, Object> getAccumulator() {
            return accumulator;
        }

        public void setAccumulator(Map<String, Object> accumulator) {
            this.accumulator = accumulator;
        }
    }
    
    public static class Info {
        private String status = "";
        private Map<String, Object> commanderMap;
        private Pong processorObj;
        private Throwable exception;

        public Info (Map<String, Object> source, Pong result) {
            this.status = "ok";
            this.commanderMap = source;
            this.processorObj = result;
        }
        
        public Info (Map<String, Object> source, Throwable exception) {
            this.status = "failed";
            this.commanderMap = source;
            this.exception = exception;
        }

        public String getStatus() {
            return status;
        }
        
        @Override
        public String toString() {
            return toString(false);
        }
        
        public String toString(boolean pretty) {
            OpflowObjectTree.Builder builder = OpflowObjectTree.buildMap().put("status", status);
            
            Map<String, Object> commanderInfo = (Map<String, Object>)commanderMap.get(OpflowConstant.COMP_COMMANDER);
            if (commanderInfo != null && processorObj != null) {
                // determines the protocol
                String protoStr = processorObj.getParameters().getOrDefault(OpflowConstant.OPFLOW_COMMON_PROTOCOL, "NONE").toString();
                // asserts the rpcMaster Map
                Map<String, Object> rpcMasterMap = null;
                if ("AMQP".equals(protoStr)) {
                    rpcMasterMap = OpflowObjectTree.assertChildMap(commanderInfo, OpflowConstant.COMP_RPC_AMQP_MASTER);
                }
                if ("HTTP".equals(protoStr)) {
                    rpcMasterMap = OpflowObjectTree.assertChildMap(commanderInfo, OpflowConstant.COMP_RPC_HTTP_MASTER);
                }
                // asserts the request Map
                if (rpcMasterMap != null) {
                    Map<String, Object> requestInfo = OpflowObjectTree.assertChildMap(rpcMasterMap, "request");
                    // copy the attributes
                    for (String key : REQUEST_ATTRS) {
                        Object attr = processorObj.getParameters().get(key);
                        if (attr != null) {
                            requestInfo.put(key, attr);
                        }
                    }
                }
                builder.put(OpflowConstant.COMP_COMMANDER, commanderInfo);
            }
            
            switch (status) {
                case "ok":
                    builder
                            .put(OpflowConstant.COMP_SERVERLET, processorObj.getAccumulator())
                            .put("summary", "The connection is ok");
                    break;
                case "failed":
                    builder
                            .put("exception", OpflowObjectTree.buildMap()
                                    .put("name", exception.getClass().getName())
                                    .put("message", exception.getMessage())
                                    .toMap())
                            .put("summary", "The workers have not been started or the parameters mismatched");
                    break;
                default:
                    builder.put("summary", "Unknown error");
                    break;
            }
            return builder.toString(pretty);
        }
    }
}
