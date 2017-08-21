package com.devebot.opflow;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowTask {
    
    public interface Listener {
        public void handleEvent();
    }
    
    public static class Countdown {
        private final Lock lock = new ReentrantLock();
        private Condition idle = lock.newCondition();
        private int total = 0;
        private int count = 0;
        private long waiting = 1000;
        
        public Countdown() {
            this(0);
        }

        public Countdown(int total) {
            this.reset(total);
        }
        
        public Countdown(int total, long waiting) {
            this.reset(total, waiting);
        }

        public final void reset(int total) {
            this.idle = lock.newCondition();
            this.count = 0;
            this.total = total;
        }
        
        public final void reset(int total, long waiting) {
            this.reset(total);
            this.waiting = waiting;
        }

        public void check() {
            lock.lock();
            try {
                count++;
                if (count >= total) idle.signal();
            } finally {
                lock.unlock();
            }
        }

        public void bingo() {
            lock.lock();
            try {
                while (count < total) idle.await();
            } catch(InterruptedException ie) {
            } finally {
                lock.unlock();
            }
            if (waiting > 0) {
                try {
                    Thread.sleep(waiting);
                } catch (InterruptedException ie) {}
            }
        }

        public int getCount() {
            return count;
        }
    }
    
    public interface Timeoutable {
        long getTimeout();
        long getTimestamp();
        void raiseTimeout();
    }
    
    public static class TimeoutMonitor {
        final Logger logger = LoggerFactory.getLogger(TimeoutMonitor.class);
        
        private long timeout;
        private final String monitorId;
        private final Map<String, ? extends Timeoutable> tasks;
        private final int interval;
        private final Timer timer = new Timer(true);
        private final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                if (tasks == null || tasks.isEmpty()) return;
                long current = OpflowUtil.getCurrentTime();
                if (logger.isDebugEnabled()) logger.debug("Monitor[" + monitorId + "].run() is invoked, current time: " + current);
                for (String key : tasks.keySet()) {
                    if (logger.isTraceEnabled()) logger.trace("Monitor[" + monitorId + "].run() examine task[" + key + "]");
                    Timeoutable task = tasks.get(key);
                    if (task == null) continue;
                    long _timeout = task.getTimeout();
                    if (_timeout <= 0) _timeout = timeout;
                    if (_timeout > 0) {
                        long diff = current - task.getTimestamp();
                        if (diff > _timeout) {
                            tasks.remove(key);
                            task.raiseTimeout();
                            if (logger.isTraceEnabled()) {
                                logger.trace("Monitor[" + monitorId + "].run() - task[" + key + "] has been timeout, will be removed");
                            }
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Monitor[" + monitorId + "].run() - task[" + key + "] is good, keep running");
                            }
                        }
                    }
                }
            }
        };
        
        public TimeoutMonitor(Map<String, ? extends Timeoutable> tasks) {
            this(tasks, 2000);
        }
        
        public TimeoutMonitor(Map<String, ? extends Timeoutable> tasks, int interval) {
            this(tasks, interval, 0l);
        }
        
        public TimeoutMonitor(Map<String, ? extends Timeoutable> tasks, int interval, long timeout) {
            this(tasks, interval, timeout, null);
        }
        
        public TimeoutMonitor(Map<String, ? extends Timeoutable> tasks, int interval, long timeout, String monitorId) {
            this.tasks = tasks;
            this.interval = interval;
            this.timeout = timeout;
            if (monitorId != null) {
                this.monitorId = monitorId;
            } else {
                this.monitorId = UUID.randomUUID().toString();
            }
            if (logger.isDebugEnabled()) logger.debug("Monitor[" + this.monitorId + "] has been created");
        }
        
        public void start() {
            if (logger.isDebugEnabled()) logger.debug("Monitor[" + monitorId + "].start()");
            if (interval > 0) {
                timer.scheduleAtFixedRate(timerTask, 0, interval);
                if (logger.isDebugEnabled()) {
                    logger.debug("Monitor[" + monitorId + "] has been started, with interval: " + this.interval);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Monitor[" + monitorId + "] is not available, because interval: " + this.interval);
                }
            }
        }
        
        public void stop() {
            if (logger.isDebugEnabled()) logger.debug("Monitor[" + monitorId + "].stop()");
            timer.cancel();
            timer.purge();
        }
    }
    
    public static class TimeoutWatcher extends Thread {

        final Logger logger = LoggerFactory.getLogger(TimeoutWatcher.class);
        
        public TimeoutWatcher(long max, Listener listener) {
            if (max > 0) {
                this.max = max;
            }
            this.listener = listener;
        }

        public TimeoutWatcher(long interval, long max, Listener listener) {
            this(max, listener);
            if (interval > 0) {
                this.interval = interval;
            }
        }

        private Listener listener;
        private long interval = 1000;
        private long max = 0;
        private long count = 0;
        private boolean done = false;

        @Override
        public void run() {
            while(count < max && !done) {
                try {
                    Thread.sleep(interval);
                    count += interval;
                    if (logger.isTraceEnabled()) logger.trace("Check " + count + "/" + max);
                    if (count >= max) {
                        if (this.listener != null) {
                            listener.handleEvent();
                        }
                        if (logger.isTraceEnabled()) logger.trace("Thread interrupted");
                        this.interrupt();
                    }
                } catch(InterruptedException ie) {}
            }
        }

        public void check() {
            this.count = 0;
        }

        public void close() {
            this.done = true;
            if (logger.isTraceEnabled()) logger.trace("Thread is closed gracefully");
        }
    }
}
