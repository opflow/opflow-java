package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowRequestSuspendException;
import com.devebot.opflow.supports.OpflowConverter;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowRpcWatcher implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    public final static long RPC_DETECTION_INTERVAL = 30000;
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcWatcher.class);
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    
    private final OpflowRpcChecker rpcChecker;
    private final boolean enabled;
    private final long interval;
    private final Timer timer = new Timer("Timer-" + OpflowRpcWatcher.class.getSimpleName(), true);
    private final MyTimerTask timerTask;
    
    public class MyTimerTask extends TimerTask {

        private boolean active = true;
        private long count = 0;

        public MyTimerTask() {
            super();
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }
        
        public long getCount() {
            return count;
        }

        @Override
        public void run() {
            if (active) {
                count++;
                OpflowLogTracer logTask = logTracer.copy();
                if (logTask.ready(LOG, Level.DEBUG)) LOG.debug(logTask
                        .put("threadCount", Thread.activeCount())
                        .text("Detector[${rpcWatcherId}].run(), threads: ${threadCount}")
                        .stringify());
                try {
                    OpflowRpcChecker.Pong result = rpcChecker.send(null);
                    if (logTask.ready(LOG, Level.DEBUG)) LOG.debug(logTask
                            .text("Detector[${rpcWatcherId}].run(), the queue is drained")
                            .stringify());
                }
                catch (OpflowRequestSuspendException e) {
                    if (logTask.ready(LOG, Level.DEBUG)) LOG.debug(logTask
                            .text("Detector[${rpcWatcherId}].run(), the valve is suspended")
                            .stringify());
                }
                catch (Throwable exception) {
                    if (logTask.ready(LOG, Level.DEBUG)) LOG.debug(logTask
                            .text("Detector[${rpcWatcherId}].run(), the queue is congested")
                            .stringify());
                }
            }
        }
    }
    
    public OpflowRpcWatcher(OpflowRpcChecker _rpcChecker) {
        this(_rpcChecker, null);
    }
    
    public OpflowRpcWatcher(OpflowRpcChecker _rpcChecker, Map<String, Object> kwargs) {
        if (kwargs == null) {
            componentId = OpflowUUID.getBase64ID();
            enabled = true;
            interval = RPC_DETECTION_INTERVAL;
        } else {
            componentId = OpflowUtil.getStringField(kwargs, CONST.COMPONENT_ID, true);
            enabled = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COMMON_ENABLED, Boolean.TRUE);
            interval = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_COMMON_INTERVAL, RPC_DETECTION_INTERVAL);
        }
        
        logTracer = OpflowLogTracer.ROOT.branch("rpcWatcherId", componentId);
        rpcChecker = _rpcChecker;
        timerTask = new MyTimerTask();
    }

    public long getCount() {
        if (timerTask == null) {
            return 0;
        }
        return timerTask.getCount();
    }
    
    public boolean isEnabled() {
        return enabled;
    }

    public long getInterval() {
        return interval;
    }

    public void start() {
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                .text("Detector[${rpcWatcherId}].start()")
                .stringify());
        if (enabled) {
            if (interval > 0) {
                timer.scheduleAtFixedRate(timerTask, 0, interval);
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .put("interval", interval)
                        .text("Detector[${rpcWatcherId}] has been started with interval: ${interval}")
                        .stringify());
            } else {
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .put("interval", interval)
                        .text("Detector[${rpcWatcherId}] is not available. undefined interval")
                        .stringify());
            }
        } else {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .text("Detector[${rpcWatcherId}] is disabled")
                    .stringify());
        }
    }

    @Override
    public void close() {
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                .text("Detector[${rpcWatcherId}].close()")
                .stringify());
        timer.cancel();
        timer.purge();
    }
}
