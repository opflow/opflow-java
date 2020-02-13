package com.devebot.opflow;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author pnhung177
 */
public class OpflowLoadAverage {
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
    
    private static class Store extends Signal {
        public double rate;

        public Store(long value, Date time) {
            super(value, time);
        }
        
        public Store(double rate, long value, Date time) {
            super(value, time);
            this.rate = rate;
        }
    }
    
    public static class Gauge {
        private final int length;
        private final Store[] stores;
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

            stores[next()] = nextNode;
        }

        public double[] export() {
            double[] result = new double[length];
            for (int i=0; i<length; i++) {
                Store item = stores[getIndex(i)];
                if (item != null) {
                    result[i] = item.rate;
                }
            }
            return result;
        }
    }
    
    public static class Meter {
        private long interval = 5000;
        private Timer timer;
        private TimerTask timerTask;
        private volatile boolean running = false;
        
        private final Map<String, Gauge> gauges = new HashMap<>();
        
        public Map<String, double[]> export() {
            Map<String, double[]> result = new HashMap<>();
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                result.put(entry.getKey(), entry.getValue().export());
            }
            return result;
        }
        
        public Meter register(String label, Source reader) {
            gauges.put(label, new Gauge(reader));
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
                            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                                entry.getValue().update();
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
