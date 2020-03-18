package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowRpcLocation {
    private String componentId;
    private String address;
    private String topic;
    private Boolean congestive = false;
    
    public enum Protocol { AMQP, HTTP };

    public OpflowRpcLocation(Protocol protocol, String componentId, String location) {
        this(protocol, componentId, location, true);
    }
    
    public OpflowRpcLocation(Protocol protocol, String componentId, String location, Boolean congestive) {
        this.componentId = componentId;
        switch (protocol) {
            case AMQP:
                this.topic = location;
            case HTTP:
                this.address = "http://" + location + "/routine";
        }
        this.congestive = congestive;
    }

    public String getComponentId() {
        return componentId;
    }

    public String getAddress() {
        return this.address;
    }

    public String getTopic() {
        return topic;
    }

    public Boolean isCongestive() {
        return congestive;
    }
}
