package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowRpcChecker {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    public static final String[] REQUEST_ATTRS = new String[] { CONST.AMQP_HEADER_ROUTINE_ID, "startTime", "endTime", "elapsedTime" };
    
    public abstract Pong send(Ping info) throws Throwable;
    
    private static String sendMethodName = null;
    
    public static String getSendMethodName() throws NoSuchMethodException {
        if (sendMethodName == null) {
            sendMethodName = OpflowRpcChecker.class.getMethod("send", Ping.class).toString();
        }
        return sendMethodName;
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
            
            Map<String, Object> commanderInfo = (Map<String, Object>)commanderMap.get(CONST.COMPNAME_COMMANDER);
            if (commanderInfo != null && processorObj != null) {
                builder.put(CONST.COMPNAME_COMMANDER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        // asserts the rpcMaster Map
                        if (!opts.containsKey(CONST.COMPNAME_RPC_MASTER) || !(opts.get(CONST.COMPNAME_RPC_MASTER) instanceof Map)) {
                            opts.put(CONST.COMPNAME_RPC_MASTER, new LinkedHashMap<String, Object>());
                        }
                        Map<String, Object> rpcMasterMap = (Map<String, Object>) opts.get(CONST.COMPNAME_RPC_MASTER);
                        // asserts the request Map
                        if (!rpcMasterMap.containsKey("request") || !(rpcMasterMap.get("request") instanceof Map)) {
                            rpcMasterMap.put("request", new LinkedHashMap<String, Object>());
                        }
                        Map<String, Object> requestInfo = (Map<String, Object>)rpcMasterMap.get("request");
                        // copy the attributes
                        for (String key : REQUEST_ATTRS) {
                            if (processorObj.getParameters().containsKey(key)) {
                                requestInfo.put(key, processorObj.getParameters().get(key));
                            }
                        }
                    }
                }, commanderInfo).toMap());
            }
            
            switch (status) {
                case "ok":
                    builder
                            .put(CONST.COMPNAME_SERVERLET, processorObj.getAccumulator())
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
