package com.devebot.opflow.supports;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowEnvTool {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowEnvTool.class);
    
    private final Map<String, String> _properties = new HashMap<>();
    private final Map<String, String> _variables = new HashMap<>();
    
    public OpflowEnvTool setSystemProperty(String key, String value) {
        _properties.put(key, value);
        return this;
    }
    
    public String getSystemProperty(String key, String def) {
        if (key == null) return null;
        if (_properties.containsKey(key)) return _properties.get(key);
        try {
            return System.getProperty(key, def);
        } catch (Throwable t) {
            if (LOG.isInfoEnabled()) LOG.info("Was not allowed to read system property [" + key + "].");
            return def;
        }
    }
    
    public OpflowEnvTool setEnvironVariable(String key, String value) {
        _variables.put(key, value);
        return this;
    }
    
    public String getEnvironVariable(String key, String def) {
        if (key == null) return null;
        if (_variables.containsKey(key)) return _variables.get(key);
        try {
            String value = System.getenv(key);
            if (value != null) return value;
            return def;
        } catch (Throwable t) {
            if (LOG.isInfoEnabled()) LOG.info("Was not allowed to read environment variable [" + key + "].");
            return def;
        }
    }
    
    public OpflowEnvTool reset() {
        _properties.clear();
        _variables.clear();
        return this;
    }
    
    public static final OpflowEnvTool instance = new OpflowEnvTool();
}
