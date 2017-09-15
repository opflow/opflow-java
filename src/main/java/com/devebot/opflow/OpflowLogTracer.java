package com.devebot.opflow;

import com.google.gson.Gson;
import java.io.InputStream;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowLogTracer {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowLogTracer.class);
    private final static String OPFLOW_VERSION = "0.1.x";
    private final static String INSTANCE_ID = OpflowUtil.getUUID();
    private final static Gson GSON = new Gson();
    
    private final Map<String, Object> fields = new LinkedHashMap<String, Object>();
    
    public OpflowLogTracer() {
        fields.put("message", null);
        fields.put("instanceId", INSTANCE_ID);
    }
    
    public OpflowLogTracer copy() {
        OpflowLogTracer target = new OpflowLogTracer();
        target.fields.putAll(fields);
        return target;
    }
    
    public OpflowLogTracer copy(String[] copied) {
        OpflowLogTracer target = copy();
        for(String key: target.fields.keySet()) {
            if (!OpflowUtil.arrayContains(copied, key)) {
                target.fields.remove(key);
            }
        }
        return target;
    }
    
    public OpflowLogTracer put(String key, String value) {
        fields.put(key, value);
        return this;
    }
    
    public Object get(String key) {
        return fields.get(key);
    }
    
    @Override
    public String toString() {
        return GSON.toJson(fields);
    }
    
    public static void bootstrap() {
        if (LOG.isInfoEnabled()) {
            LOG.info(new OpflowLogTracer()
                    .put("message", "Opflow Library Information")
                    .put("lib_name", "opflow-java")
                    .put("lib_version", getVersionNameFromManifest())
                    .put("os_name", System.getProperty("os.name"))
                    .put("os_version", System.getProperty("os.version"))
                    .put("os_arch", System.getProperty("os.arch"))
                    .toString());
        }
    }
    
    private static String getVersionNameFromPOM() {
        try {
            Properties props = new Properties();
            String POM_PROPSFILE = "META-INF/maven/com.devebot.opflow/opflow-core/pom.properties";
            props.load(OpflowLogTracer.class.getClassLoader().getResourceAsStream(POM_PROPSFILE));
            return props.getProperty("version");
        } catch (Exception ioe) {}
        return OPFLOW_VERSION;
    }
    
    private static String getVersionNameFromManifest() {
        try {
            InputStream manifestStream = OpflowLogTracer.class.getClassLoader().getResourceAsStream("META-INF/MANIFEST.MF");
            if (manifestStream != null) {
                Manifest manifest = new Manifest(manifestStream);
                Attributes attributes = manifest.getMainAttributes();
                return attributes.getValue("Implementation-Version");
            }
        } catch (Exception ioe) {}
        return OPFLOW_VERSION;
    }
    
    private String getVersionNameFromManifestBackup() {
        Class clazz = OpflowLogTracer.class;
        String className = clazz.getSimpleName() + ".class";
        String classPath = clazz.getResource(className).toString();
        String manifestPath = classPath.replace("com/devebot/opflow/OpflowLogTracer.class", "META-INF/MANIFEST.MF");
        try {
            Manifest manifest = new Manifest(new URL(manifestPath).openStream());
            Attributes attributes = manifest.getMainAttributes();
            return attributes.getValue("Implementation-Version");
        } catch (Exception ioe) {}
        return OPFLOW_VERSION;
    }
    
    static {
        bootstrap();
    }
}
