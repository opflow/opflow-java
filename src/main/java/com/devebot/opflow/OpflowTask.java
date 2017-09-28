package com.devebot.opflow;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
        private final static Logger LOG = LoggerFactory.getLogger(TimeoutMonitor.class);
        private final OpflowLogTracer logTracer;
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
                OpflowLogTracer logTask = logTracer.branch("timestamp", current);
                if (LOG.isDebugEnabled()) LOG.debug(logTask
                        .put("taskListSize", tasks.size())
                        .put("message", "run() is invoked")
                        .toString());
                logTask.reset();
                for (String key : tasks.keySet()) {
                    if (LOG.isTraceEnabled()) LOG.trace(logTask
                            .put("taskId", key)
                            .put("message", "run() examine a task")
                            .toString());
                    Timeoutable task = tasks.get(key);
                    if (task == null) continue;
                    long _timeout = task.getTimeout();
                    if (_timeout <= 0) _timeout = timeout;
                    if (_timeout > 0) {
                        long diff = current - task.getTimestamp();
                        if (diff > _timeout) {
                            tasks.remove(key);
                            task.raiseTimeout();
                            if (LOG.isTraceEnabled()) LOG.trace(logTask
                                    .put("taskId", key)
                                    .put("message", "run() task is timeout, will be removed")
                                    .toString());
                        } else {
                            if (LOG.isTraceEnabled()) LOG.trace(logTask
                                    .put("taskId", key)
                                    .put("message", "run() task is good, keep running")
                                    .toString());
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
            this.monitorId = (monitorId != null) ? monitorId : OpflowUtil.getLogID();
            logTracer = OpflowLogTracer.ROOT.branch("monitorId", this.monitorId);
            if (LOG.isDebugEnabled()) LOG.debug(logTracer
                    .put("message", "Monitor has been created")
                    .toString());
        }
        
        public void start() {
            if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                    .put("message", "Monitor.start()")
                    .toString());
            if (interval > 0) {
                timer.scheduleAtFixedRate(timerTask, 0, interval);
                if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                        .put("interval", interval)
                        .put("message", "Monitor has been started")
                        .toString());
            } else {
                if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                        .put("interval", interval)
                        .put("message", "Monitor is not available. undefined interval")
                        .toString());
            }
        }
        
        public void stop() {
            if (LOG.isDebugEnabled()) LOG.debug(logTracer.reset()
                    .put("message", "Monitor.stop()")
                    .toString());
            timer.cancel();
            timer.purge();
        }
    }
    
    public static class TimeoutWatcher extends Thread {
        private final static Logger LOG = LoggerFactory.getLogger(TimeoutWatcher.class);
        private final OpflowLogTracer logTracer;
        
        public TimeoutWatcher(String requestId, long max, Listener listener) {
            this.requestId = requestId;
            logTracer = OpflowLogTracer.ROOT.branch("requestId", this.requestId);
            if (max > 0) {
                this.max = max;
            }
            this.listener = listener;
        }

        public TimeoutWatcher(String taskId, long interval, long max, Listener listener) {
            this(taskId, max, listener);
            if (interval > 0) {
                this.interval = interval;
            }
        }

        private String requestId;
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
                    if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                            .put("message", "TimeoutWatcher is interrupted")
                            .toString());
                    if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                            .put("count", count)
                            .put("max", max)
                            .put("message", "TimeoutWatcher.listener is requested")
                            .toString());
                    if (count >= max) {
                        if (this.listener != null) {
                            listener.handleEvent();
                        }
                        if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                                .put("message", "TimeoutWatcher.listener is requested")
                                .toString());
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
            if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                    .put("message", "TimeoutWatcher is closed gracefully")
                    .toString());
        }
    }
}
