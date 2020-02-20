package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowMathUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

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
    
    public static class Top {
        public double rate;
        public Date time;

        public Top(double rate, Date time) {
            this.rate = rate;
            this.time = time;
        }
    }
    
    public static class Info {
        public Top finest;
        public double[] speeds;
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
        private Source reader = null;

        public Gauge(int size) {
            this(size, null);
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
            return current;
        }

        public void update() {
            if (reader != null) {
                update(new Signal(reader.getValue(), reader.getTime()));
            }
        }

        public void update(Signal point) {
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

        public Info export() {
            Info result = new Info();
            // extract the top
            if (top != null) {
                result.finest = new Top(OpflowMathUtil.round(top.rate, 1), top.time);
            }
            // generate the speeds
            result.speeds = new double[length];
            for (int i=0; i<length; i++) {
                Store item = stores[getIndex(i)];
                if (item != null) {
                    result.speeds[i] = OpflowMathUtil.round(item.rate, 1);
                }
            }
            return result;
        }
        
        public Top getTop() {
            return new Top(top.rate, top.time);
        }
    }
    
    public static class Meter {
        private long interval = INTERVAL_DEFAULT;
        private int length = TAIL_LENGTH_DEFAULT;
        private Timer timer;
        private TimerTask timerTask;
        private volatile boolean running = false;
        private volatile boolean active = false;

        private final Map<String, Gauge> gauges = new HashMap<>();

        public Meter(Map<String, Object> kwargs) {
            kwargs = OpflowUtil.ensureNotNull(kwargs);
            // load [active] value from the config, false by default
            if (kwargs.get("active") instanceof Boolean) {
                active = (Boolean) kwargs.get("active");
            }
            // updating interval
            if (kwargs.get("interval") instanceof Long) {
                long _interval = (Long) kwargs.get("interval");
                if (_interval > 0) {
                    interval = _interval;
                }
            }
            // customize the tail length
            if (kwargs.get("length") instanceof Integer) {
                int _length = (Integer) kwargs.get("length");
                if (_length > 0 && _length <= 10) {
                    length = _length;
                }
            }
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public Map<String, Object> export() {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                result.put(entry.getKey(), OpflowObjectTree.buildMap()
                        .put("throughput", entry.getValue().export())
                        .toMap());
            }
            return result;
        }

        public Meter register(String label, Source reader) {
            gauges.put(label, new Gauge(length, reader));
            return this;
        }

        public synchronized void start() {
            if (!this.running) {
                if (this.timer == null) {
                    this.timer = new Timer("Timer-" + OpflowUtil.extractClassName(Meter.class), true);
                }
                if (this.timerTask == null) {
                    this.timerTask = new TimerTask() {
                        @Override
                        public void run() {
                            if (isActive()) {
                                for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                                    entry.getValue().update();
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
                timerTask.cancel();
                timerTask = null;
                timer.cancel();
                timer.purge();
                timer = null;
                running = false;
            }
        }
    }
}
