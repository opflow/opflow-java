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
    
    public boolean available() {
        return true;
    }
    
    public abstract Info locate();
}
