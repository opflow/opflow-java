package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowHelper {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowHelper.class);
    
    public static OpflowRpcMaster createRpcMaster() throws OpflowConstructorException {
        return createRpcMaster(null, null);
    }
    
    public static OpflowRpcMaster createRpcMaster(String propFile) throws OpflowConstructorException {
        return createRpcMaster(propFile, null);
    }
    
    public static OpflowRpcMaster createRpcMaster(Properties defaultProps) throws OpflowConstructorException {
        return createRpcMaster(null, defaultProps);
    }
    
    public static OpflowRpcMaster createRpcMaster(String propFile, Properties defaultProps) throws OpflowConstructorException {
        if (LOG.isTraceEnabled()) LOG.trace("Create new OpflowRpcMaster with properties file: " + propFile);
        
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
        
        if (LOG.isTraceEnabled()) LOG.trace("OpflowRpcMaster has been created successfully");
        
        return new OpflowRpcMaster(params);
    }
    
    public static OpflowRpcWorker createRpcWorker() throws OpflowConstructorException {
        return createRpcWorker(null, null);
    }
    
    public static OpflowRpcWorker createRpcWorker(String propFile) throws OpflowConstructorException {
        return createRpcWorker(propFile, null);
    }
    
    public static OpflowRpcWorker createRpcWorker(Properties defaultProps) throws OpflowConstructorException {
        return createRpcWorker(null, defaultProps);
    }
    
    public static OpflowRpcWorker createRpcWorker(String propFile, Properties defaultProps) throws OpflowConstructorException {
        if (LOG.isTraceEnabled()) LOG.trace("Create new OpflowRpcWorker with properties file: " + propFile);
        
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
        
        if (LOG.isTraceEnabled()) LOG.trace("OpflowRpcWorker has been created successfully");
        
        return new OpflowRpcWorker(params);
    }
    
    public static OpflowPubsubHandler createPubsubHandler() throws OpflowConstructorException {
        return createPubsubHandler(null, null);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(String propFile) throws OpflowConstructorException {
        return createPubsubHandler(propFile, null);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(Properties defaultProps) throws OpflowConstructorException {
        return createPubsubHandler(null, defaultProps);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(String propFile, Properties defaultProps) throws OpflowConstructorException {
        if (LOG.isTraceEnabled()) LOG.trace("Create new OpflowPubsubHandler with properties file: " + propFile);
        
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
        
        if (LOG.isTraceEnabled()) LOG.trace("OpflowPubsubHandler has been created successfully");
        
        return new OpflowPubsubHandler(params);
    }
    
    public static Properties loadProperties() throws OpflowConstructorException {
        return loadProperties(null, null);
    }
    
    public static Properties loadProperties(String propFile) throws OpflowConstructorException {
        return loadProperties(propFile, null);
    }
            
    public static Properties loadProperties(String propFile, Properties props) throws OpflowConstructorException {
        try {
            if (props == null) {
                if (propFile == null) propFile = "opflow.properties";
                props = new Properties();
            } else {
                props = new Properties(props);
            }
            if (propFile != null) {
                InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propFile);
                if (inputStream == null) {
                    throw new FileNotFoundException("property file '" + propFile + "' not found in the classpath");
                }
                props.load(inputStream);
            }
            return props;
        } catch (IOException exception) {
            throw new OpflowConstructorException(exception);
        }
    }
}
