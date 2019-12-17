package com.devebot.opflow.supports;

/**
 *
 * @author acegik
 */
public interface OpflowRpcChecker {

    Pong send(Ping info);
    
    public static class Ping {
        
    }
    
    public static class Pong {
        
    }
}
