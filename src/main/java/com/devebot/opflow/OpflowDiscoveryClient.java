package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowDiscoveryClient {
    public interface Info {
        String getUri();
        
        default String getVersion() {
            return null;
        }
        
        default Map<String, Object> getOptions() {
            return null;
        }
    }
    
    public interface Locator {
        default boolean available() {
            return true;
        }
        Info locate();
    }
}
