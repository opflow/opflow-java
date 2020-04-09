package com.devebot.opflow.supports;

/**
 *
 * @author acegik
 */
public class OpflowConfigUtil {
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    
    public String getConfigValue(String key, String def) {
        String str = ENVTOOL.getSystemProperty(key, null);
        if (str == null) {
            str = ENVTOOL.getEnvironVariable(OpflowStringUtil.convertCamelCaseToSnakeCase(key), null);
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
