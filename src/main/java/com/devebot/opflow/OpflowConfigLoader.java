package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.parser.ParserException;
import org.yaml.snakeyaml.scanner.ScannerException;

/**
 *
 * @author acegik
 */
public class OpflowConfigLoader {
    public final static String DEFAULT_CONFIGURATION_KEY = "opflow.configuration";
    public final static String DEFAULT_CONFIGURATION_ENV = "OPFLOW_CONFIGURATION";
    public final static String DEFAULT_CONFIGURATION_FILE = "opflow.properties";

    private final static Logger LOG = LoggerFactory.getLogger(OpflowConfigLoader.class);
    private final static OpflowLogTracer LOG_TRACER = OpflowLogTracer.ROOT.copy();
    
    public static Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
        return loadConfiguration(null, null, true);
    }
    
    public static Map<String, Object> loadConfiguration(String configFile) throws OpflowBootstrapException {
        return loadConfiguration(null, configFile, configFile == null);
    }
    
    public static Map<String, Object> loadConfiguration(Map<String, Object> config, String configFile) throws OpflowBootstrapException {
        return loadConfiguration(config, configFile, configFile == null && config == null);
    }
    
    public static Map<String, Object> loadConfiguration(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        try {
            Yaml yaml = new Yaml();
            if (config == null) {
                config = new HashMap<>();
            }
            if (configFile != null || useDefaultFile) {
                URL url = getConfigurationUrl(configFile);
                if (url != null) {
                    String ext = getConfigurationExtension(url);
                    if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                            .put("configFile", url.getFile())
                            .put("extension", ext)
                            .text("load configuration file")
                            .stringify());
                    if ("yml".equals(ext) || "yaml".equals(ext)) {
                        Map<String,Object> yamlConfig = (Map<String,Object>)yaml.load(url.openStream());
                        mergeConfiguration(config, yamlConfig);
                    } else if ("properties".equals(ext)) {
                        Properties props = new Properties();
                        props.load(url.openStream());
                        mergeConfiguration(config, props);
                    }
                } else {
                    configFile = (configFile != null) ? configFile : DEFAULT_CONFIGURATION_FILE;
                    throw new FileNotFoundException("configuration file '" + configFile + "' not found");
                }
            }
            if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                    .put("YAML", OpflowJsontool.toString(config))
                    .text("loaded properties content")
                    .stringify());
            return config;
        } catch (IOException exception) {
            throw new OpflowBootstrapException(exception);
        } catch (ParserException | ScannerException exception) {
            throw new OpflowBootstrapException(exception);
        } catch (MarkedYAMLException exception) {
            throw new OpflowBootstrapException(exception);
        }
    }
    
    private static URL getConfigurationUrl(String configFile) {
        URL url;
        String cfgFromSystem = OpflowUtil.getSystemProperty(DEFAULT_CONFIGURATION_KEY, null);
        if (cfgFromSystem == null) {
            cfgFromSystem = configFile;
        }
        if (cfgFromSystem == null) {
            cfgFromSystem = OpflowUtil.getEnvironVariable(DEFAULT_CONFIGURATION_ENV, null);
        }
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", cfgFromSystem)
                .text("detected configuration file")
                .stringify());
        if (cfgFromSystem == null) {
            url = OpflowUtil.getResource(DEFAULT_CONFIGURATION_FILE);
            if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", url)
                .text("default configuration url")
                .stringify());
        } else {
            try {
                url = new URL(cfgFromSystem);
            } catch (MalformedURLException ex) {
                // woa, the cfgFromSystem string is not a URL,
                // attempt to get the resource from the class path
                url = OpflowUtil.getResource(cfgFromSystem);
            }
        }
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", url)
                .text("final configuration url")
                .stringify());
        return url;
    }
    
    public static String getConfigurationExtension(URL url) {
        String filename = url.getFile();
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
    
    public static Map<String, Object> mergeConfiguration(Map<String, Object> target, Map<String, Object> source) {
        if (target == null) target = new HashMap<>();
        if (source == null) return target;
        for (String key : source.keySet()) {
            if (source.get(key) instanceof Map && target.get(key) instanceof Map) {
                Map<String, Object> targetChild = (Map<String, Object>) target.get(key);
                Map<String, Object> sourceChild = (Map<String, Object>) source.get(key);
                target.put(key, mergeConfiguration(targetChild, sourceChild));
            } else {
                target.put(key, source.get(key));
            }
        }
        return target;
    }
    
    public static Map<String, Object> mergeConfiguration(Map<String, Object> target, Properties props) {
        if (target == null) target = new HashMap<>();
        if (props == null) return target;
        Set<Object> keys = props.keySet();
        for(Object keyo:keys) {
            String key = (String) keyo;
            String[] path = key.split("\\.");
            if (path.length > 0) {
                Map<String, Object> current = target;
                for(int i=0; i<(path.length - 1); i++) {
                    if (!current.containsKey(path[i]) || !(current.get(path[i]) instanceof Map)) {
                        current.put(path[i], new HashMap<String, Object>());
                    }
                    current = (Map<String, Object>)current.get(path[i]);
                }
                current.put(path[path.length - 1], props.getProperty(key));
            }
        }
        return target;
    }
    
    public static Properties loadProperties() throws OpflowBootstrapException {
        Properties props = new Properties();
        URL url = OpflowUtil.getResource(DEFAULT_CONFIGURATION_FILE);
        try {
            if (url != null) props.load(url.openStream());
        } catch (IOException exception) {
            throw new OpflowBootstrapException(exception);
        }
        return props;
    }
}
