package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowEnvTool;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.xml.bind.DatatypeConverter;

/**
 *
 * @author pnhung177
 */
public class OpflowUUID {
    private static final boolean UUID_PRE_GENERATED;
    private static final boolean UUID_FORMAT_BASE64;
    private static final boolean UUID_AUTOSTART_GENERATOR;
    private static final Generator UUID_GENERATOR;
    
    static {
        UUID_PRE_GENERATED = !"false".equals(getEnvProperty("OPFLOW_UUID_PRE_GENERATED", null));;
        UUID_FORMAT_BASE64 = !"false".equals(getEnvProperty("OPFLOW_UUID_FORMAT_BASE64", null));
        UUID_AUTOSTART_GENERATOR = !"false".equals(getEnvProperty("OPFLOW_UUID_GENERATOR_AUTORUN", null));
        UUID_GENERATOR = new Generator();
    }
    
    public static String getUUID() {
        return UUID.randomUUID().toString();
    }
    
    public static String getBase64ID() {
        if (!UUID_PRE_GENERATED) {
            if (!UUID_FORMAT_BASE64) {
                return getUUID();
            }
            return convertUUIDToBase64(UUID.randomUUID());
        }
        return UUID_GENERATOR.pick();
    }
    
    public static String getBase64ID(String uuid) {
        if (!UUID_FORMAT_BASE64) return uuid;
        if (uuid == null) return getBase64ID();
        return convertUUIDToBase64(UUID.fromString(uuid));
    }
    
    private static String convertUUIDToBase64(UUID uuid) {
        // Create byte[] for base64 from uuid
        byte[] src = ByteBuffer.wrap(new byte[16])
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
        // Encode to Base64 and remove trailing ==
        return DatatypeConverter.printBase64Binary(src)
                .replace('/', '-')
                .substring(0, 22);
    }
    
    private static class Generator implements AutoCloseable {
        private final Timer timer = new Timer("Timer-" + extractClassName(), true);
        private final TimerTask timerTask;
        private final ConcurrentLinkedQueue<String> store = new ConcurrentLinkedQueue<>();
        private final long interval;
        private volatile boolean running = false;
        
        public Generator() {
            this.interval = 2000l;
            this.timerTask = new TimerTask() {
                @Override
                public void run() {
                    prepare(50, 500);
                }
            };
            if (UUID_AUTOSTART_GENERATOR) {
                this.start();
            }
        }

        public void prepare(int bound, int total) {
            if (store.size() < bound) {
                store.addAll(generate(total));
            }
        }

        public String pick() {
            prepare(10, 20);
            return store.poll();
        }

        private List<String> generate(int number) {
            List<String> buff = new ArrayList<>(number);
            for (int i=0; i<number; i++) {
                if (!UUID_FORMAT_BASE64) {
                    buff.add(UUID.randomUUID().toString());
                    continue;
                }
                buff.add(convertUUIDToBase64(UUID.randomUUID()));
            }
            return buff;
        }

        public synchronized void start() {
            if (!running) {
                this.timer.scheduleAtFixedRate(this.timerTask, 0, this.interval);
                running = true;
            }
        }
        
        @Override
        public synchronized void close() throws Exception {
            if (running) {
                timer.cancel();
                timer.purge();
                running = false;
            }
        }

        private static String extractClassName() {
            return Generator.class.getName().replace(Generator.class.getPackage().getName(), "");
        }
    }
    
    private static String getEnvProperty(String name, String defval) {
        String val = OpflowEnvTool.instance.getSystemProperty(name, null);
        if (val != null) return val;
        val = OpflowEnvTool.instance.getEnvironVariable(name, null);
        if (val != null) return val;
        return defval;
    }
}
