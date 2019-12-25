package com.devebot.opflow;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowRpcChecker {

    public abstract Pong send(Ping info) throws Throwable;
    
    private static String sendMethodName = "";
    
    public static String getSendMethodName() throws NoSuchMethodException {
        if (sendMethodName.length() == 0) {
            sendMethodName = OpflowRpcChecker.class.getMethod("send", Ping.class).toString();
        }
        return sendMethodName;
    }
    
    public static class Ping {
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
        private Map<String, Object> source;
        private Pong result;
        private Throwable exception;

        public Info (Map<String, Object> source, Pong result) {
            this.status = "ok";
            this.source = source;
            this.result = result;
        }
        
        public Info (Map<String, Object> source, Throwable exception) {
            this.status = "failed";
            this.source = source;
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
            OpflowUtil.MapBuilder builder = OpflowUtil.buildOrderedMap()
                    .put("status", status)
                    .put("commander", source);
            if (result != null) {
                if (!source.containsKey("request") || !(source.get("request") instanceof HashMap)) {
                    source.put("request", OpflowUtil.buildOrderedMap().toMap());
                }
                Map<String, Object> requestInfo = (Map<String, Object>)source.get("request");
                if (result.getParameters().containsKey("requestId")) {
                    requestInfo.put("requestId", result.getParameters().get("requestId"));
                }
                if (result.getParameters().containsKey("startTime")) {
                    requestInfo.put("startTime", result.getParameters().get("startTime"));
                }
                if (result.getParameters().containsKey("endTime")) {
                    requestInfo.put("endTime", result.getParameters().get("endTime"));
                }
                if (result.getParameters().containsKey("duration")) {
                    requestInfo.put("duration", result.getParameters().get("duration"));
                }
            }
            switch (status) {
                case "ok":
                    builder
                            .put("serverlet", result.getAccumulator())
                            .put("summary", "The connection is ok");
                    break;
                case "failed":
                    builder
                            .put("exception", OpflowUtil.buildOrderedMap()
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
