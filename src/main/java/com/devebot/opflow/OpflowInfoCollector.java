package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author acegik
 */
public interface OpflowInfoCollector {
    public static final String SCOPE_BASIC = "BASIC";
    public static final String SCOPE_FULL = "FULL";

    Map<String, Object> collect();
    Map<String, Object> collect(String scope);
    Map<String, Object> collect(Map<String, Boolean> flags);
}
