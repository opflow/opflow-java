package com.devebot.opflow;

import com.google.gson.Gson;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
    private final static Gson GSON = new Gson();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowLogTracer.class);
    private final static String OPFLOW_VERSION = "0.1.x";
    private final static String OPFLOW_INSTANCE_ID = OpflowUtil.getUUID();
    
    private final static int RESET_MODE;
    private final static boolean KEEP_ORDER;
    static {
        String treepath = OpflowUtil.getSystemProperty("OPFLOW_LOGTREEPATH", null);
        if ("none".equals(treepath)) RESET_MODE = 0;
        else if ("parent".equals(treepath)) RESET_MODE = 1;
        else if ("full".equals(treepath)) RESET_MODE = 2;
        else RESET_MODE = 2;

        KEEP_ORDER = (OpflowUtil.getSystemProperty("OPFLOW_LOGKEEPORDER", null) == null);
    }
    
    private final OpflowLogTracer parent;
    private final String key;
    private final Object value;
    private final Map<String, Object> fields;
    
    public final static OpflowLogTracer ROOT = new OpflowLogTracer();
    
    public OpflowLogTracer() {
        this(null, "instanceId", OpflowUtil.getSystemProperty("OPFLOW_INSTANCE_ID", OPFLOW_INSTANCE_ID));
    }
    
    private OpflowLogTracer(OpflowLogTracer ref, String key, Object value) {
        this.parent = ref;
        this.key = key;
        this.value = value;
        this.fields = KEEP_ORDER ? new LinkedHashMap<String, Object>() : new HashMap<String, Object>();
        this.reset();
    }
    
    public OpflowLogTracer copy() {
        return new OpflowLogTracer(this.parent, this.key, this.value);
    }
    
    public OpflowLogTracer branch(String key, Object value) {
        return new OpflowLogTracer(this, key, value);
    }
    
    public final OpflowLogTracer reset(int mode) {
        this.fields.clear();
        this.fields.put("message", null);
        if (mode > 0) {
            if (mode == 1) {
                if (this.parent != null) {
                    this.fields.put(this.parent.key, this.parent.value);
                }
            } else {
                OpflowLogTracer ref = this.parent;
                while(ref != null) {
                    this.fields.put(ref.key, ref.value);
                    ref = ref.parent;
                }
            }
        }
        this.fields.put(key, value);
        return this;
    }
    
    public final OpflowLogTracer reset() {
        return this.reset(RESET_MODE);
    }
    
    public OpflowLogTracer put(String key, Object value) {
        fields.put(key, value);
        return this;
    }
    
    public Object get(String key) {
        return fields.get(key);
    }
    
    @Override
    public String toString() {
        return stringify();
    }
    
    public String stringify() {
        return stringify(false);
    }
    
    public String stringify(boolean reset) {
        return stringify(reset, RESET_MODE);
    }
    
    public String stringify(boolean reset, int mode) {
        String jsonStr = GSON.toJson(fields);
        if (INTERCEPTOR_ENABLED && !interceptors.isEmpty()) {
            Map<String, Object> cloned = new HashMap<String, Object>();
            cloned.putAll(fields);
            for(StringifyInterceptor interceptor:interceptors) {
                interceptor.intercept(cloned);
            }
        }
        if (reset) this.reset(mode);
        return jsonStr;
    }
    
    
    private final static boolean INTERCEPTOR_ENABLED;
    static {
        INTERCEPTOR_ENABLED = !"false".equals(OpflowUtil.getSystemProperty("OPFLOW_DEBUGLOG", null));
    }
    
    public interface StringifyInterceptor {
        public void intercept(Map<String, Object> logdata);
    }
    
    private static List<StringifyInterceptor> interceptors = new ArrayList<StringifyInterceptor>();
    
    public static boolean addStringifyInterceptor(StringifyInterceptor interceptor) {
        if (interceptor != null && interceptors.indexOf(interceptor) < 0) {
            interceptors.add(interceptor);
            return true;
        }
        return false;
    }
    
    public static boolean removeStringifyInterceptor(StringifyInterceptor interceptor) {
        return interceptors.remove(interceptor);
    }
    
    private static String libraryInfo = null;;
    
    public static String getLibraryInfo() {
        if (libraryInfo == null) {
            libraryInfo = new OpflowLogTracer()
                    .put("message", "Opflow Library Information")
                    .put("lib_name", "opflow-java")
                    .put("lib_version", getVersionNameFromManifest())
                    .put("os_name", System.getProperty("os.name"))
                    .put("os_version", System.getProperty("os.version"))
                    .put("os_arch", System.getProperty("os.arch"))
                    .toString();
        }
        return libraryInfo;
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
    
    static {
        if (LOG.isInfoEnabled()) LOG.info(getLibraryInfo());
    }
}
