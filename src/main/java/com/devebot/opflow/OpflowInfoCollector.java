package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author acegik
 */
public interface OpflowInfoCollector {
    public static final String SCOPE_PING = "PING";
    public static final String SCOPE_INFO = "INFO";
    public static final String SCOPE_THROUGHPUT = "THROUGHPUT";
    public static final String SCOPE_LATEST_SPEED = "LATEST_SPEED";
    public static final String SCOPE_MESSAGE_RATE = "MESSAGE_RATE";

    Map<String, Object> collect();
    Map<String, Object> collect(String scope);
    Map<String, Object> collect(Map<String, Boolean> flags);
    Map<String, Object> traffic(Map<String, Boolean> flags);
}
