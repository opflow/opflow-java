package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowLogTracer;
import com.devebot.opflow.OpflowUtil;
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
    public final static long RPC_DETECTION_INTERVAL = 30000;
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcWatcher.class);
    
    private final String instanceId;
    private final OpflowLogTracer logTracer;
    
    private final OpflowRpcChecker rpcChecker;
    private final boolean enabled;
    private final long interval;
    private final Timer timer = new Timer(true);
    private final TimerTask timerTask;
    
    private boolean congested = false;
    
    public OpflowRpcWatcher(OpflowRpcChecker _rpcChecker) {
        this(_rpcChecker, null);
    }
    
    public OpflowRpcWatcher(OpflowRpcChecker _rpcChecker, Map<String, Object> kwargs) {
        if (kwargs == null) {
            instanceId = OpflowUtil.getLogID();
            enabled = true;
            interval = RPC_DETECTION_INTERVAL;
        } else {
            instanceId = OpflowUtil.getOptionField(kwargs, "commanderId", true);
            enabled = OpflowConverter.convert(OpflowUtil.getOptionField(kwargs, "enabled", Boolean.TRUE), Boolean.class);
            interval = OpflowConverter.convert(OpflowUtil.getOptionField(kwargs, "interval", RPC_DETECTION_INTERVAL), Long.class);
        }
        
        this.logTracer = OpflowLogTracer.ROOT.branch("rpcWatcherId", this.instanceId);
        this.rpcChecker = _rpcChecker;
        this.timerTask = new TimerTask() {
            @Override
            public void run() {
                long current = OpflowUtil.getCurrentTime();
                OpflowLogTracer logTask = logTracer.branch("timestamp", current);
                if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTask
                        .put("threadCount", Thread.activeCount())
                        .text("Detector[${rpcSwitcherId}].run(), threads: ${threadCount}")
                        .stringify());
                try {
                    OpflowRpcChecker.Pong result = rpcChecker.send(new OpflowRpcChecker.Ping());
                    congested = false;
                    if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTask
                            .text("Detector[${rpcSwitcherId}].run(), the queue is drained")
                            .stringify());
                } catch (Throwable exception) {
                    congested = true;
                    if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTask
                            .text("Detector[${rpcSwitcherId}].run(), the queue is congested")
                            .stringify());
                }
            }
        };
    }
    
    public boolean isCongested() {
        return congested;
    }
    
    public void setCongested(boolean _congested) {
        congested = _congested;
    }
    
    public void start() {
        if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                .text("Detector.start()")
                .stringify());
        if (enabled) {
            if (interval > 0) {
                timer.scheduleAtFixedRate(timerTask, 0, interval);
                if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                        .put("interval", interval)
                        .text("Detector has been started with interval: ${interval}")
                        .stringify());
            } else {
                if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                        .put("interval", interval)
                        .text("Detector is not available. undefined interval")
                        .stringify());
            }
        } else {
            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                    .text("Detector is disabled")
                    .stringify());
        }
    }

    @Override
    public void close() {
        if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logTracer
                .text("Detector.close()")
                .stringify());
        timer.cancel();
        timer.purge();
    }
}
