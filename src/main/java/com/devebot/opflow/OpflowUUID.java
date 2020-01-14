package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowEnvTool;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.xml.bind.DatatypeConverter;

/**
 *
 * @author pnhung177
 */
public class OpflowUUID {
    private static final boolean OPFLOW_PREGEN_UUID = true;
    private static final boolean OPFLOW_BASE64UUID;
    private static final Generator generator;
    
    static {
        OPFLOW_BASE64UUID = !"false".equals(OpflowEnvTool.instance.getSystemProperty("OPFLOW_BASE64UUID", null)) &&
                !"false".equals(OpflowEnvTool.instance.getEnvironVariable("OPFLOW_BASE64UUID", null));
        generator = new Generator();
    }
    
    public static String getUUID() {
        return UUID.randomUUID().toString();
    }
    
    public static String getLogID() {
        if (!OPFLOW_PREGEN_UUID) {
            if (!OPFLOW_BASE64UUID) getUUID();
            return convertUUIDToBase64(UUID.randomUUID());
        }
        return generator.pick();
    }
    
    public static String getLogID(String uuid) {
        if (!OPFLOW_BASE64UUID) return uuid;
        if (uuid == null) return getLogID();
        return convertUUIDToBase64(UUID.fromString(uuid));
    }
    
    private static String convertUUIDToBase64(UUID uuid) {
        // Create byte[] for base64 from uuid
        byte[] src = ByteBuffer.wrap(new byte[16])
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        // Encode to Base64 and remove trailing ==
        return DatatypeConverter.printBase64Binary(src).substring(0, 22);
    }
    
    private static class Generator implements AutoCloseable {

        private final ConcurrentLinkedQueue<String> store = new ConcurrentLinkedQueue<>();
        
        public String pick() {
            if (store.size() < 10) {
                store.addAll(generate(100));
            }
            return store.poll();
        }
        
        private List<String> generate(int number) {
            List<String> buff = new ArrayList<>(number);
            for (int i=0; i<number; i++) {
                if (!OPFLOW_BASE64UUID) {
                    buff.add(UUID.randomUUID().toString());
                    continue;
                }
                buff.add(convertUUIDToBase64(UUID.randomUUID()));
            }
            return buff;
        }
        
        @Override
        public void close() throws Exception {
        }
    }
}
