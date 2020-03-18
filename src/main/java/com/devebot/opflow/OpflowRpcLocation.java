package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowRpcLocation {
    private Protocol protocol;
    private String componentId;
    private String address;
    private String topic;
    private Boolean congestive = null;
    
    public enum Protocol { AMQP, HTTP };

    public OpflowRpcLocation(Protocol protocol, String componentId, String location) {
        this(protocol, componentId, location, null);
    }
    
    public OpflowRpcLocation(Protocol protocol, String componentId, String location, Boolean congestive) {
        this.protocol = protocol;
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
        if (congestive == null) {
            return false;
        }
        return congestive;
    }

    public void setCongestive(Boolean congestive) {
        this.congestive = congestive;
    }
    
    public OpflowRpcLocation update(OpflowRpcLocation n) {
        if (n != null && n.protocol == this.protocol) {
            if (n.componentId != null) {
                this.componentId = n.componentId;
            }
            if (n.address != null) {
                this.address = n.address;
            }
            if (n.topic != null) {
                this.topic = n.topic;
            }
            if (n.congestive != null) {
                this.congestive = n.congestive;
            }
        }
        return this;
    }
}
