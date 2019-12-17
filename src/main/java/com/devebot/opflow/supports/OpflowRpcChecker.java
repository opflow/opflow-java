package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowUtil;
import java.util.Map;

/**
 *
 * @author acegik
 */
public interface OpflowRpcChecker {

    Pong send(Ping info);
    
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
        private Pong pong;

        public Info (Pong pong) {
            this.status = "ok";
            this.pong = pong;
        }
        
        public Info (Exception exception) {
            this.status = "failed";
        }

        public String getStatus() {
            return status;
        }
        
        @Override
        public String toString() {
            return toString(false);
        }
        
        public String toString(boolean pretty) {
            String summary;
            switch (status) {
                case "ok":
                    summary = "The connection is ok";
                    break;
                case "failed":
                    summary = "The workers have not been started or the parameters mismatched";
                    break;
                default:
                    summary = "Unknown error";
                    break;
            }
            return OpflowUtil.buildMap()
                    .put("status", status)
                    .put("serverlet", pong.getAccumulator())
                    .put("summary", summary)
                    .toString(pretty);
        }
    }
}
