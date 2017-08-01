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
        
        params.put("exchangeName", props.get("opflow.master.exchangeName"));
        params.put("routingKey", props.get("opflow.master.routingKey"));
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
        
        params.put("exchangeName", props.get("opflow.worker.exchangeName"));
        params.put("routingKey", props.get("opflow.worker.routingKey"));
        params.put("operatorName", props.get("opflow.worker.operatorName"));
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
        
        params.put("exchangeName", props.get("opflow.pubsub.exchangeName"));
        params.put("routingKey", props.get("opflow.pubsub.routingKey"));
        params.put("subscriberName", props.get("opflow.pubsub.subscriberName"));
        
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
