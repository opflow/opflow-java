package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author acegik
 */
interface OpflowInfoCollector {
    public static enum Scope {
        BASIC,
        FULL;
    }
    Map<String, Object> collect();
    Map<String, Object> collect(Scope scope);
}
