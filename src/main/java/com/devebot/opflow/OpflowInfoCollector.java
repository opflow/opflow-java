package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author acegik
 */
public interface OpflowInfoCollector {
    public static final String SCOPE_PING = "PING";
    public static final String SCOPE_INFO = "INFO";
    public static final String SCOPE_LOAD_AVERAGE = "LOAD_AVERAGE";
    public static final String SCOPE_MESSAGE_RATE = "MESSAGE_RATE";
    public static final String SCOPE_THROUGHPUT = "THROUGHPUT";

    Map<String, Object> collect();
    Map<String, Object> collect(String scope);
    Map<String, Object> collect(Map<String, Boolean> flags);
    Map<String, Object> traffic(Map<String, Boolean> flags);
}
