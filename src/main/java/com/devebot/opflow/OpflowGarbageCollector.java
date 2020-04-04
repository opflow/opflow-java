package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowGarbageCollector {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowGarbageCollector.class);
    
    private final static long DEFAULT_PERIOD = 5L * 60L * 1000L;
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final long period;
    private long milestone;

    public OpflowGarbageCollector(Map<String, Object> kwargs) {
        componentId = OpflowUtil.getStringField(kwargs, CONST.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("garbageCollectorId", componentId);
        
        Long _period = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_COMMON_INTERVAL, null);
        if (_period == null) {
            _period = DEFAULT_PERIOD * 2;
        } else {
            if (_period < DEFAULT_PERIOD) {
                _period = DEFAULT_PERIOD;
            }
        }
        this.period = _period;
        
        reset();
    }

    public boolean clean() {
        long diff = (new Date()).getTime() - milestone;
        if (diff >= period) {
            Runtime.getRuntime().gc();
            reset();
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("diff", diff)
                    .put("period", period)
                    .text("GarbageCollector[${garbageCollectorId}].clean() - different time: ${diff}")
                    .stringify());
            return true;
        }
        return false;
    }

    private void reset() {
        milestone = (new Date()).getTime();
    }
}
