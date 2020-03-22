package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowRpcRoutingInfo {
    private OpflowConstant.Protocol protocol;
    private String componentId;
    private String address;
    private String topic;
    private Boolean congestive = null;
    
    private String url = null;

    public OpflowRpcRoutingInfo(OpflowConstant.Protocol protocol, String componentId, String location) {
        this(protocol, componentId, location, null);
    }
    
    public OpflowRpcRoutingInfo(OpflowConstant.Protocol protocol, String componentId, String location, Boolean congestive) {
        this.protocol = protocol;
        this.componentId = componentId;
        switch (protocol) {
            case AMQP:
                this.topic = location;
                break;
            case HTTP:
                this.address = location;
                break;
        }
        this.congestive = congestive;
    }

    public String getComponentId() {
        return componentId;
    }

    public String getAddress() {
        if (this.url == null) {
            this.url = "http://" + this.address + "/routine";
        }
        return this.url;
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
    
    public OpflowRpcRoutingInfo update(OpflowRpcRoutingInfo n) {
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
