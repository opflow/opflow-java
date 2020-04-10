package com.devebot.opflow.supports;

/**
 *
 * @author acegik
 */
public class OpflowConfigUtil {
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    
    public static String getConfigValue(String key) {
        return getConfigValue(key, null);
    }
    
    public static String getConfigValue(String key, String def) {
        String str = ENVTOOL.getSystemProperty(key, null);
        if (str == null) {
            key = OpflowStringUtil.convertDottedNameToSnakeCase(key);
            str = ENVTOOL.getEnvironVariable(OpflowStringUtil.convertCamelCaseToSnakeCase(key), null);
        }
        if (str == null) {
            str = ENVTOOL.getEnvironVariable(OpflowStringUtil.convertCamelCaseToSnakeCase(key, false), null);
        }
        if (str == null) {
            str = ENVTOOL.getEnvironVariable(key.toUpperCase(), null);
        }
        if (str == null) {
            str = ENVTOOL.getEnvironVariable(key, def);
        }
        return str;
    }
}
