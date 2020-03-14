package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowDiscoveryNativeAgent extends OpflowDiscoveryClient {

    private final OpflowRpcObserver rpcObserver;
    private final Info info = new Info() {
        @Override
        public String getUri() {
            if (rpcObserver == null) {
                return null;
            }
            return "http://" + rpcObserver.getLatestAddress() + "/routine";
        }
    };
    
    public OpflowDiscoveryNativeAgent(OpflowRpcObserver rpcObserver) {
        this.rpcObserver = rpcObserver;
    }
    
    @Override
    public Info locate() {
        return info;
    }
}
