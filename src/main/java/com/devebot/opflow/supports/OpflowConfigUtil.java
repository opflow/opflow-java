package com.devebot.opflow.supports;

/**
 *
 * @author acegik
 */
public class OpflowConfigUtil {
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    
    public static String getConfigValue(String key) {
        return getConfigValue(key, null, false);
    }
    
    public static String getConfigValue(String key, String def) {
        return getConfigValue(key, def, false);
    }
    
    public static String getConfigValue(String key, String def, boolean envarOnly) {
        String str = null;
        
        if (!envarOnly) {
            str = ENVTOOL.getSystemProperty(key, null);
            if (str != null) return str;
        }
        
        key = OpflowStringUtil.convertDottedNameToSnakeCase(key);
        str = ENVTOOL.getEnvironVariable(key, null);
        if (str != null) return str;
        
        str = ENVTOOL.getEnvironVariable(OpflowStringUtil.convertCamelCaseToSnakeCase(key), null);
        if (str != null) return str;
        
        str = ENVTOOL.getEnvironVariable(OpflowStringUtil.convertCamelCaseToSnakeCase(key, false), null);
        if (str != null) return str;
        
        str = ENVTOOL.getEnvironVariable(key.toUpperCase(), null);
        if (str != null) return str;
        
        return def;
    }
}
