package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowRpcChecker {
    public static final String[] REQUEST_ATTRS = new String[] { "requestId", "startTime", "endTime", "elapsedTime" };
    
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
            OpflowObjectTree.MapBuilder builder = OpflowObjectTree.buildOrderedMap().put("status", status);
            
            Map<String, Object> commanderInfo = (Map<String, Object>)commanderMap.get("commander");
            if (commanderInfo != null && processorObj != null) {
                builder.put("commander", OpflowObjectTree.buildOrderedMap(new OpflowObjectTree.MapListener() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        // asserts the rpcMaster Map
                        if (!opts.containsKey("rpcMaster") || !(opts.get("rpcMaster") instanceof Map)) {
                            opts.put("rpcMaster", new LinkedHashMap<String, Object>());
                        }
                        Map<String, Object> rpcMasterMap = (Map<String, Object>) opts.get("rpcMaster");
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
                            .put("serverlet", processorObj.getAccumulator())
                            .put("summary", "The connection is ok");
                    break;
                case "failed":
                    builder
                            .put("exception", OpflowObjectTree.buildOrderedMap()
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
