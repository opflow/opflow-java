package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
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
        
        if (props.getProperty("opflow.master.uri") != null) {
            params.put("uri", props.getProperty("opflow.master.uri"));
        } else {
            params.put("uri", props.getProperty("opflow.uri"));
        }
        
        if (props.getProperty("opflow.master.exchangeName") != null) {
            params.put("exchangeName", props.getProperty("opflow.master.exchangeName"));
        } else {
            params.put("exchangeName", props.getProperty("opflow.exchangeName"));
        }
        
        if (props.getProperty("opflow.master.routingKey") != null) {
            params.put("routingKey", props.getProperty("opflow.master.routingKey"));
        } else {
            params.put("routingKey", props.getProperty("opflow.routingKey"));
        }
        
        if (props.getProperty("opflow.master.applicationId") != null) {
            params.put("applicationId", props.getProperty("opflow.master.applicationId"));
        } else {
            params.put("applicationId", props.getProperty("opflow.applicationId"));
        }
        
        params.put("responseName", props.getProperty("opflow.master.responseName"));
        
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
        
        if (props.getProperty("opflow.worker.uri") != null) {
            params.put("uri", props.getProperty("opflow.worker.uri"));
        } else {
            params.put("uri", props.getProperty("opflow.uri"));
        }
        
        if (props.getProperty("opflow.worker.exchangeName") != null) {
            params.put("exchangeName", props.getProperty("opflow.worker.exchangeName"));
        } else {
            params.put("exchangeName", props.getProperty("opflow.exchangeName"));
        }
        
        if (props.getProperty("opflow.worker.routingKey") != null) {
            params.put("routingKey", props.getProperty("opflow.worker.routingKey"));
        } else {
            params.put("routingKey", props.getProperty("opflow.routingKey"));
        }
        
        if (props.getProperty("opflow.worker.applicationId") != null) {
            params.put("applicationId", props.getProperty("opflow.worker.applicationId"));
        } else {
            params.put("applicationId", props.getProperty("opflow.applicationId"));
        }
        
        if (props.getProperty("opflow.worker.operatorName") != null) {
            params.put("operatorName", props.getProperty("opflow.worker.operatorName"));
        } else {
            params.put("operatorName", props.getProperty("opflow.queueName"));
        }
        
        params.put("responseName", props.getProperty("opflow.worker.responseName"));
        
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
        
        if (props.getProperty("opflow.pubsub.uri") != null) {
            params.put("uri", props.getProperty("opflow.pubsub.uri"));
        } else {
            params.put("uri", props.getProperty("opflow.uri"));
        }
        
        if (props.getProperty("opflow.pubsub.exchangeName") != null) {
            params.put("exchangeName", props.getProperty("opflow.pubsub.exchangeName"));
        } else {
            params.put("exchangeName", props.getProperty("opflow.exchangeName"));
        }
        
        if (props.getProperty("opflow.pubsub.routingKey") != null) {
            params.put("routingKey", props.getProperty("opflow.pubsub.routingKey"));
        } else {
            params.put("routingKey", props.getProperty("opflow.routingKey"));
        }
        
        if (props.getProperty("opflow.pubsub.applicationId") != null) {
            params.put("applicationId", props.getProperty("opflow.pubsub.applicationId"));
        } else {
            params.put("applicationId", props.getProperty("opflow.applicationId"));
        }
        
        params.put("otherKeys", props.getProperty("opflow.pubsub.otherKeys"));
        
        if (props.getProperty("opflow.pubsub.subscriberName") != null) {
            params.put("subscriberName", props.getProperty("opflow.pubsub.subscriberName"));
        } else {
            params.put("subscriberName", props.getProperty("opflow.queueName"));
        }
        
        params.put("recyclebinName", props.getProperty("opflow.pubsub.recyclebinName"));
        
        String redeliveredLimit = props.getProperty("opflow.pubsub.redeliveredLimit");
        if (redeliveredLimit != null) {
            try {
                params.put("redeliveredLimit", Integer.parseInt(redeliveredLimit));
            } catch (NumberFormatException nfe) {
                if (LOG.isTraceEnabled()) LOG.trace("createPubsubHandler() - redeliveredLimit is not a number");
            }
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
            if (LOG.isTraceEnabled()) LOG.trace("[-] Properties: " + getPropertyAsString(props));
            return props;
        } catch (IOException exception) {
            throw new OpflowConstructorException(exception);
        }
    }
    
    private static String getPropertyAsString(Properties prop) {
        StringWriter writer = new StringWriter();
        prop.list(new PrintWriter(writer));
        return writer.getBuffer().toString();
    }
}
