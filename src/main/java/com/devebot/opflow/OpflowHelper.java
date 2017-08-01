package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author drupalex
 */
public class OpflowHelper {
    
    public static OpflowRpcMaster createRpcMaster() throws OpflowConstructorException {
        return createRpcMaster(null);
    }
    
    public static OpflowRpcMaster createRpcMaster(String propFile) throws OpflowConstructorException {
        return createRpcMaster(propFile, null);
    }
    
    public static OpflowRpcMaster createRpcMaster(String propFile, Properties defaultProps) throws OpflowConstructorException {
        Properties props = loadProperties(propFile, defaultProps);
        Map<String, Object> params = new HashMap<String, Object>();
        
        if (props.get("opflow.master.uri") != null) {
            params.put("uri", props.get("opflow.master.uri"));
        } else {
            params.put("uri", props.get("opflow.uri"));
        }
        
        if (props.get("opflow.master.exchangeName") != null) {
            params.put("exchangeName", props.get("opflow.master.exchangeName"));
        } else {
            params.put("exchangeName", props.get("opflow.exchangeName"));
        }
        
        if (props.get("opflow.master.routingKey") != null) {
            params.put("routingKey", props.get("opflow.master.routingKey"));
        } else {
            params.put("routingKey", props.get("opflow.routingKey"));
        }
        
        params.put("responseName", props.get("opflow.master.responseName"));
        
        return new OpflowRpcMaster(params);
    }
    
    public static OpflowRpcWorker createRpcWorker() throws OpflowConstructorException {
        return createRpcWorker(null);
    }
    
    public static OpflowRpcWorker createRpcWorker(String propFile) throws OpflowConstructorException {
        return createRpcWorker(propFile, null);
    }
    
    public static OpflowRpcWorker createRpcWorker(String propFile, Properties defaultProps) throws OpflowConstructorException {
        Properties props = loadProperties(propFile, defaultProps);
        Map<String, Object> params = new HashMap<String, Object>();
        
        if (props.get("opflow.worker.uri") != null) {
            params.put("uri", props.get("opflow.worker.uri"));
        } else {
            params.put("uri", props.get("opflow.uri"));
        }
        
        if (props.get("opflow.worker.exchangeName") != null) {
            params.put("exchangeName", props.get("opflow.worker.exchangeName"));
        } else {
            params.put("exchangeName", props.get("opflow.exchangeName"));
        }
        
        if (props.get("opflow.worker.routingKey") != null) {
            params.put("routingKey", props.get("opflow.worker.routingKey"));
        } else {
            params.put("routingKey", props.get("opflow.routingKey"));
        }
        
        if (props.get("opflow.worker.operatorName") != null) {
            params.put("operatorName", props.get("opflow.worker.operatorName"));
        } else {
            params.put("operatorName", props.get("opflow.queueName"));
        }
        
        params.put("responseName", props.get("opflow.worker.responseName"));
        
        return new OpflowRpcWorker(params);
    }
    
    public static OpflowPubsubHandler createPubsubHandler() throws OpflowConstructorException {
        return createPubsubHandler(null);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(String propFile) throws OpflowConstructorException {
        return createPubsubHandler(propFile, null);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(String propFile, Properties defaultProps) throws OpflowConstructorException {
        Properties props = loadProperties(propFile, defaultProps);
        Map<String, Object> params = new HashMap<String, Object>();
        
        if (props.get("opflow.pubsub.uri") != null) {
            params.put("uri", props.get("opflow.pubsub.uri"));
        } else {
            params.put("uri", props.get("opflow.uri"));
        }
        
        if (props.get("opflow.pubsub.exchangeName") != null) {
            params.put("exchangeName", props.get("opflow.pubsub.exchangeName"));
        } else {
            params.put("exchangeName", props.get("opflow.exchangeName"));
        }
        
        if (props.get("opflow.pubsub.routingKey") != null) {
            params.put("routingKey", props.get("opflow.pubsub.routingKey"));
        } else {
            params.put("routingKey", props.get("opflow.routingKey"));
        }
        
        params.put("otherKeys", props.get("opflow.pubsub.otherKeys"));
        
        if (props.get("opflow.pubsub.subscriberName") != null) {
            params.put("subscriberName", props.get("opflow.pubsub.subscriberName"));
        } else {
            params.put("subscriberName", props.get("opflow.queueName"));
        }
        
        return new OpflowPubsubHandler(params);
    }
    
    private static Properties loadProperties(String propFile, Properties props) throws OpflowConstructorException {
        try {
            if (propFile == null) propFile = "opflow.properties";
            if (props == null) {
                props = new Properties();
            } else {
                props = new Properties(props);
            }
            InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propFile);
            if (inputStream == null) {
                throw new FileNotFoundException("property file '" + propFile + "' not found in the classpath");
            }
            props.load(inputStream);
            return props;
        } catch (IOException exception) {
            throw new OpflowConstructorException(exception);
        }
    }
}
