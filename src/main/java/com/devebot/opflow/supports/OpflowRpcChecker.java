package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowUtil;
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
        private Map<String, Object> accumulator;

        public Pong() {
        }
        
        public Pong(Map<String, Object> accumulator) {
            this.accumulator = accumulator;
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
            switch (status) {
                case "ok":
                    builder.put("serverlet", result.getAccumulator())
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
