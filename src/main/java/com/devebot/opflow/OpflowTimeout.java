package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowDateTime;
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
public class OpflowTimeout {
    
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
    
    public static class Monitor implements AutoCloseable {
        private final static Logger LOG = LoggerFactory.getLogger(Monitor.class);
        private final OpflowLogTracer logTracer;
        private long timeout;
        private final String monitorId;
        private final Map<String, ? extends Timeoutable> tasks;
        private final int interval;
        private final Timer timer = new Timer("Timer-" + extractClassName(), true);
        private final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                if (tasks == null || tasks.isEmpty()) return;
                long current = OpflowDateTime.getCurrentTime();
                OpflowLogTracer logTask = logTracer.branch("timestamp", current);
                if (logTask.ready(LOG, Level.DEBUG)) LOG.debug(logTask
                        .put("taskListSize", tasks.size())
                        .put("threadCount", Thread.activeCount())
                        .text("Monitor[${monitorId}].run(), tasks: ${taskListSize}, threads: ${threadCount}")
                        .stringify());
                for (String key : tasks.keySet()) {
                    if (logTask.ready(LOG, Level.TRACE)) LOG.trace(logTask
                            .put("taskId", key)
                            .text("Monitor[${monitorId}].run() examine the task[${taskId}]")
                            .stringify());
                    Timeoutable task = tasks.get(key);
                    if (task == null) continue;
                    long _timeout = task.getTimeout();
                    if (_timeout <= 0) _timeout = timeout;
                    if (logTask.ready(LOG, Level.TRACE)) LOG.trace(logTask
                            .put("taskId", key)
                            .put("monitorTimeout", timeout)
                            .put("taskTimeout", task.getTimeout())
                            .put("timeout", _timeout)
                            .text("Monitor[${monitorId}].run() task[${taskId}]'s timeout: ${taskTimeout} | ${monitorTimeout} ~ ${timeout}")
                            .stringify());
                    if (_timeout > 0) {
                        long diff = current - task.getTimestamp();
                        if (diff > _timeout) {
                            tasks.remove(key);
                            task.raiseTimeout();
                            if (logTask.ready(LOG, Level.TRACE)) LOG.trace(logTask
                                    .put("taskId", key)
                                    .put("diff", diff)
                                    .put("timeout", _timeout)
                                    .text("Monitor[${monitorId}].run() task[${taskId}] is timeout (diff: ${diff} > ${timeout}), rejected")
                                    .stringify());
                        } else {
                            if (logTask.ready(LOG, Level.TRACE)) LOG.trace(logTask
                                    .put("taskId", key)
                                    .text("Monitor[${monitorId}].run() task[${taskId}] is good, keep running")
                                    .stringify());
                        }
                    }
                }
            }
        };
        
        public Monitor(Map<String, ? extends Timeoutable> tasks) {
            this(tasks, 2000);
        }
        
        public Monitor(Map<String, ? extends Timeoutable> tasks, int interval) {
            this(tasks, interval, 1000l);
        }
        
        public Monitor(Map<String, ? extends Timeoutable> tasks, int interval, long timeout) {
            this(tasks, interval, timeout, null);
        }
        
        public Monitor(Map<String, ? extends Timeoutable> tasks, int interval, long timeout, String monitorId) {
            this.tasks = tasks;
            this.interval = interval;
            this.timeout = timeout;
            this.monitorId = (monitorId != null) ? monitorId : OpflowUUID.getBase64ID();
            logTracer = OpflowLogTracer.ROOT.branch("monitorId", this.monitorId);
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("interval", this.interval)
                    .put("timeout", this.timeout)
                    .text("Monitor[${monitorId}] has been created with interval: ${interval}, timeout: ${timeout}")
                    .stringify());
        }
        
        public void start() {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .text("Monitor[${monitorId}].start()")
                    .stringify());
            if (interval > 0) {
                timer.scheduleAtFixedRate(timerTask, 0, interval);
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .put("interval", interval)
                        .text("Monitor[${monitorId}] has been started with interval: ${interval}")
                        .stringify());
            } else {
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .put("interval", interval)
                        .text("Monitor[${monitorId}] is not available (the interval is undefined)")
                        .stringify());
            }
        }
        
        @Override
        public void close() {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .text("Monitor[${monitorId}].close()")
                    .stringify());
            timer.cancel();
            timer.purge();
        }
        
        private static String extractClassName() {
            return Monitor.class.getName().replace(Monitor.class.getPackage().getName(), "");
        }
    }
    
    public static class Watcher extends Thread {
        private final static Logger LOG = LoggerFactory.getLogger(Watcher.class);
        private final OpflowLogTracer logTracer;
        
        public Watcher(String requestId, long max, Listener listener) {
            this.requestId = requestId;
            logTracer = OpflowLogTracer.ROOT.branch("requestId", this.requestId);
            if (max > 0) {
                this.max = max;
            }
            this.listener = listener;
        }

        public Watcher(String taskId, long interval, long max, Listener listener) {
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
                    if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                            .text("Watcher is interrupted")
                            .stringify());
                    if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                            .put("count", count)
                            .put("max", max)
                            .text("Watcher.listener is requested")
                            .stringify());
                    if (count >= max) {
                        if (this.listener != null) {
                            listener.handleEvent();
                        }
                        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                                .text("Watcher.listener is requested")
                                .stringify());
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
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                    .text("Watcher is closed gracefully")
                    .stringify());
        }
    }
}
