package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowMathUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author pnhung177
 */
public class OpflowThroughput {
    public static final long INTERVAL_DEFAULT = 5000l;
    public static final int TAIL_LENGTH_DEFAULT = 3;

    public interface Source {
        long getValue();
        Date getTime();
    }

    public static class Signal {
        public long value;
        public Date time;

        public Signal(long value, Date time) {
            this.value = value;
            this.time = time;
        }
    }
    
    public static class Speed {
        public double rate;
        public Date time;

        public Speed(double rate, Date time) {
            this.rate = rate;
            this.time = time;
        }
    }
    
    public static class Info {
        public Speed[] timeline;
        public Speed top;
    }
    
    private static class Store extends Signal {
        public double rate;

        public Store(long value, Date time) {
            super(value, time);
        }
        
        public Store(long value, Date time, double rate) {
            super(value, time);
            this.rate = rate;
        }
    }
    
    public static class Gauge {
        private final int length;
        private final Store[] stores;
        private Store top = null;
        private int current = 0;
        private int total = 0;
        private Source reader = null;

        public Gauge(int length) {
            this(length, null);
        }
        
        public Gauge(Source source) {
            this(TAIL_LENGTH_DEFAULT, source);
        }
        
        public Gauge(int length, Source source) {
            this.length = length;
            this.current = 0;
            this.stores = new Store[length];
            this.reader = source;
        }

        private int getIndex(int i) {
            int real = current + i;
            while (real >= length) {
                real -= length;
            }
            return real;
        }

        private int next() {
            current--;
            if (current < 0) {
                current = length - 1;
            }
            if (total < length) {
                total++;
            }
            return current;
        }

        public void update() {
            if (reader != null) {
                update(new Signal(reader.getValue(), reader.getTime()));
            }
        }

        public synchronized void update(Signal point) {
            if (point == null) {
                return;
            }

            Store nextNode = new Store(point.value, point.time);
            if (nextNode.value < 0 || nextNode.time == null) {
                return;
            }

            Store prevNode = stores[current];
            if (prevNode != null) {
                long period = nextNode.time.getTime() - prevNode.time.getTime();
                if (period <= 0) {
                    return;
                }
                nextNode.rate = 1000.0 * (nextNode.value - prevNode.value) / period;
            }

            if (top == null) {
                top = nextNode;
            } else {
                if (top.rate < nextNode.rate) {
                    top = nextNode;
                }
            }

            stores[next()] = nextNode;
        }
        
        public synchronized void reset() {
            top = null;
            for (int i=0; i<length; i++) {
                stores[i] = null;
            }
        }
        
        public Info export() {
            return export(total);
        }
        
        public Info export(int len) {
            Info result = new Info();
            // extract the top
            if (top != null) {
                result.top = new Speed(OpflowMathUtil.round(top.rate, 1), top.time);
            }
            // generate the timeline
            if (len <= 0) {
                len = 1;
            }
            if (len > total) {
                len = total;
            }
            result.timeline = new Speed[len];
            for (int i=0; i<len; i++) {
                Store item = stores[getIndex(i)];
                if (item != null) {
                    result.timeline[i] = new Speed(OpflowMathUtil.round(item.rate, 1), item.time);
                }
            }
            return result;
        }
        
        public Speed getTop() {
            return new Speed(top.rate, top.time);
        }
    }
    
    public static class Meter {
        private final static OpflowConstant CONST = OpflowConstant.CURRENT();
        private final static Logger LOG = LoggerFactory.getLogger(Meter.class);

        private long interval = INTERVAL_DEFAULT;
        private int length = TAIL_LENGTH_DEFAULT;
        private Timer timer;
        private TimerTask timerTask;
        private volatile boolean running = false;
        private volatile boolean active = false;

        private final String componentId;
        private final OpflowLogTracer logTracer;
        private final Object lock = new Object();
        private final Map<String, Gauge> gauges = new HashMap<>();

        public Meter(Map<String, Object> kwargs) {
            kwargs = OpflowObjectTree.ensureNonNull(kwargs);
            // initialize the logTracer
            componentId = OpflowUtil.getStringField(kwargs, CONST.COMPONENT_ID, true);
            logTracer = OpflowLogTracer.ROOT.branch("speedMeterId", this.componentId);
            // load [active] value from the config, false by default
            active = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COMMON_ACTIVE, Boolean.FALSE);
            // updating interval
            Long _interval = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_COMMON_INTERVAL, null);
            if (_interval != null && _interval > 0) {
                interval = _interval;
            }
            // customize the tail length
            Integer _length = OpflowUtil.getIntegerField(kwargs, OpflowConstant.OPFLOW_COMMON_LENGTH, null);
            if (_length != null && _length > 0 && _length <= 10) {
                length = _length;
            }
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public Map<String, Object> getMetadata() {
            return OpflowObjectTree.buildMap()
                    .put("throughput", OpflowObjectTree.buildMap()
                            .put(OpflowConstant.OPFLOW_COMMON_ACTIVE, active)
                            .put(OpflowConstant.OPFLOW_COMMON_INTERVAL, interval)
                            .put(OpflowConstant.OPFLOW_COMMON_LENGTH, length)
                            .toMap())
                    .toMap();
        }

        public Map<String, Object> export() {
            return export(length);
        }

        public Map<String, Object> export(int len) {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                result.put(entry.getKey(), OpflowObjectTree.buildMap()
                        .put("throughput", entry.getValue().export(len))
                        .toMap());
            }
            return result;
        }

        public Meter register(String label, Source reader) {
            gauges.put(label, new Gauge(length, reader));
            return this;
        }

        public void reset() {
            if (isActive()) {
                synchronized (lock) {
                    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                        entry.getValue().reset();
                    }
                }
            }
        }
        
        public synchronized void serve() {
            if (!this.running) {
                if (this.timer == null) {
                    this.timer = new Timer("Timer-" + OpflowUtil.extractClassName(Meter.class), true);
                }
                if (this.timerTask == null) {
                    this.timerTask = new TimerTask() {
                        @Override
                        public void run() {
                            if (isActive()) {
                                synchronized (lock) {
                                    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                                        entry.getValue().update();
                                    }
                                }
                            }
                        }
                    };
                }
                this.timer.scheduleAtFixedRate(this.timerTask, 0, this.interval);
                this.running = true;
            }
        }

        public synchronized void close() {
            if (running) {
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .text("SpeedMeter[${speedMeterId}].close()")
                        .stringify());
                timerTask.cancel();
                timerTask = null;
                timer.cancel();
                timer.purge();
                timer = null;
                running = false;
                if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                        .text("SpeedMeter[${speedMeterId}].close() end!")
                        .stringify());
            }
        }
    }
}
